// Command genproto is a cross-platform contract generation tool for the mfdh
// project. It bootstraps the toolchain (protoc + Go generator plugins) into
// ./bin/ and generates Go code and an OpenAPI spec from api/proto/v1 into
// api/gen/.
//
// It replaces the Windows-only PowerShell pipeline (scripts/gen-proto.ps1):
// one codebase works on Windows / macOS / Linux.
//
// Usage:
//
//	go run ./cmd/genproto                 # bootstrap toolchain and generate
//	go run ./cmd/genproto -skip-bootstrap # regenerate with an existing toolchain
//	go run ./cmd/genproto -only-bootstrap # only install/verify the toolchain
package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Pinned versions (verified 2026-08-16).
const (
	modulePath    = "github.com/NicoYazawa/microservice_diagnosis"
	protocVersion = "35.1" // protocolbuffers/protobuf
)

// pluginDef describes a Go generator plugin installed via `go install`.
type pluginDef struct {
	name    string
	pkg     string
	version string
}

var pluginDefs = []pluginDef{
	{"protoc-gen-go", "google.golang.org/protobuf/cmd/protoc-gen-go", "v1.36.12"},
	// protoc-gen-go-grpc is a nested module in grpc-go since v1.83; it has its own version line.
	{"protoc-gen-go-grpc", "google.golang.org/grpc/cmd/protoc-gen-go-grpc", "v1.6.2"},
	{"protoc-gen-grpc-gateway", "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway", "v2.30.0"},
	{"protoc-gen-openapiv2", "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2", "v2.30.0"},
}

func main() {
	skipBootstrap := flag.Bool("skip-bootstrap", false, "skip toolchain bootstrap (regenerate only)")
	onlyBootstrap := flag.Bool("only-bootstrap", false, "only install/verify the toolchain, do not generate")
	flag.Parse()

	root, err := repoRoot()
	if err != nil {
		fatal(fmt.Errorf("locate repo root: %w", err))
	}
	binDir := filepath.Join(root, "bin")

	if !*skipBootstrap {
		if err := ensureProtoc(root, binDir); err != nil {
			fatal(fmt.Errorf("bootstrap protoc: %w", err))
		}
		if err := ensurePlugins(binDir); err != nil {
			fatal(fmt.Errorf("bootstrap plugins: %w", err))
		}
	} else {
		if err := verifyToolchain(binDir); err != nil {
			fatal(err)
		}
		fmt.Println("[genproto] toolchain verified (bootstrap skipped)")
	}

	if *onlyBootstrap {
		fmt.Println("[genproto] toolchain ready")
		return
	}
	if err := generate(root, binDir); err != nil {
		fatal(err)
	}
}

// repoRoot walks up from the current directory to find go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

// ensureProtoc downloads and unpacks protoc for the current platform if missing.
func ensureProtoc(root, binDir string) error {
	protocDir := filepath.Join(binDir, "protoc-"+protocVersion)
	protocExe := filepath.Join(protocDir, "bin", "protoc"+exeSuffix())
	if fileExists(protocExe) {
		fmt.Printf("[genproto] protoc %s ready\n", protocVersion)
		return nil
	}
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}
	platform := protocPlatform()
	url := fmt.Sprintf("https://github.com/protocolbuffers/protobuf/releases/download/v%s/protoc-%s-%s.zip",
		protocVersion, protocVersion, platform)
	zipPath := filepath.Join(binDir, fmt.Sprintf("protoc-%s-%s.zip", protocVersion, platform))
	fmt.Printf("[genproto] downloading protoc %s (%s) ...\n", protocVersion, platform)
	if err := download(url, zipPath); err != nil {
		return err
	}
	if err := unzip(zipPath, protocDir); err != nil {
		return err
	}
	if err := os.Remove(zipPath); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(protocExe, 0o755); err != nil {
			return err
		}
	}
	fmt.Printf("[genproto] protoc installed: %s\n", protocExe)
	return nil
}

// ensurePlugins installs the Go generator plugins into binDir via `go install`,
// skipping any plugin whose version marker matches the pinned version.
func ensurePlugins(binDir string) error {
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}
	for _, p := range pluginDefs {
		exe := filepath.Join(binDir, p.name+exeSuffix())
		marker := filepath.Join(binDir, p.name+".version")
		if fileExists(exe) && readFileTrim(marker) == p.version {
			fmt.Printf("[genproto] %s %s ready\n", p.name, p.version)
			continue
		}
		fmt.Printf("[genproto] go install %s@%s ...\n", p.pkg, p.version)
		cmd := exec.Command("go", "install", p.pkg+"@"+p.version)
		cmd.Env = append(os.Environ(), "GOBIN="+binDir)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("go install %s: %w", p.pkg, err)
		}
		if err := os.WriteFile(marker, []byte(p.version), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// verifyToolchain checks that a previously bootstrapped toolchain is present.
func verifyToolchain(binDir string) error {
	protocExe := filepath.Join(binDir, "protoc-"+protocVersion, "bin", "protoc"+exeSuffix())
	if !fileExists(protocExe) {
		return fmt.Errorf("protoc missing: run once without -skip-bootstrap first")
	}
	for _, p := range pluginDefs {
		if !fileExists(filepath.Join(binDir, p.name+exeSuffix())) {
			return fmt.Errorf("plugin %s missing: run once without -skip-bootstrap first", p.name)
		}
	}
	return nil
}

// generate runs protoc over the three contracts, writing Go code and OpenAPI
// into api/gen/. Plugins are passed by absolute path (no PATH dependency).
func generate(root, binDir string) error {
	openapiOut := filepath.Join(root, "api", "gen", "openapi")
	if err := os.MkdirAll(openapiOut, 0o755); err != nil {
		return err
	}

	protoc := filepath.Join(binDir, "protoc-"+protocVersion, "bin", "protoc"+exeSuffix())
	args := []string{
		"-I", filepath.Join(root, "api", "proto"),
		"-I", filepath.Join(root, "third_party"),
		fmt.Sprintf("--go_out=%s", root),
		fmt.Sprintf("--go_opt=module=%s", modulePath),
		fmt.Sprintf("--go-grpc_out=%s", root),
		fmt.Sprintf("--go-grpc_opt=module=%s", modulePath),
		fmt.Sprintf("--grpc-gateway_out=%s", root),
		fmt.Sprintf("--grpc-gateway_opt=module=%s", modulePath),
		fmt.Sprintf("--openapiv2_out=%s", openapiOut),
		"--openapiv2_opt=allow_merge=true",
		"--openapiv2_opt=merge_file_name=mfdh",
	}
	for _, p := range pluginDefs {
		args = append(args, fmt.Sprintf("--plugin=%s=%s", p.name, filepath.Join(binDir, p.name+exeSuffix())))
	}
	args = append(args, "v1/observation.proto", "v1/orchestrator.proto", "v1/agent.proto")

	fmt.Println("[genproto] generating Go code + OpenAPI ...")
	cmd := exec.Command(protoc, args...)
	cmd.Dir = root
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("protoc: %w", err)
	}

	fmt.Println("[genproto] generated files:")
	return filepath.Walk(filepath.Join(root, "api", "gen"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, path)
			fmt.Printf("  + %s\n", filepath.ToSlash(rel))
		}
		return nil
	})
}

// protocPlatform maps runtime.GOOS/GOARCH to the protobuf release asset suffix.
func protocPlatform() string {
	arch := runtime.GOARCH
	switch arch {
	case "amd64":
		arch = "x86_64"
	case "arm64":
		arch = "aarch_64"
	}
	switch runtime.GOOS {
	case "windows":
		if arch == "x86_64" {
			return "win64"
		}
		return "win32"
	case "darwin":
		return "osx-" + arch
	default:
		return runtime.GOOS + "-" + arch
	}
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// download fetches url into dest.
func download(url, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %s", url, resp.Status)
	}
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	return err
}

// unzip extracts src zip archive into dest.
func unzip(src, dest string) error {
	zr, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer zr.Close()
	for _, f := range zr.File {
		target := filepath.Join(dest, f.Name)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.Create(target)
		if err != nil {
			rc.Close()
			return err
		}
		if _, err := io.Copy(out, rc); err != nil {
			out.Close()
			rc.Close()
			return err
		}
		out.Close()
		rc.Close()
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "genproto: %v\n", err)
	os.Exit(1)
}
