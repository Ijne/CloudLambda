package sandbox

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"lambda/internal/ns"
	"lambda/internal/params"
	"lambda/internal/vars"
)

type Config struct {
	EnvType    vars.EnvType
	CodeSource string

	MemoryLimit int64
	CPUQuota    int64
	TimeLimit   time.Duration
}

type Result struct {
	ExitCode int
	TimedOut bool
	Stdout   []byte
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open src %s: %w", src, err)
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("stat src %s: %w", src, err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create dst dir: %w", err)
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return fmt.Errorf("create dst %s: %w", dst, err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("copy data: %w", err)
	}

	return nil
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("create dst dir %s: %w", dst, err)
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("read src dir %s: %w", src, err)
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		info, err := os.Lstat(srcPath)
		if err != nil {
			return fmt.Errorf("lstat %s: %w", srcPath, err)
		}

		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(srcPath)
			if err != nil {
				return fmt.Errorf("read symlink %s: %w", srcPath, err)
			}
			if err := os.Symlink(linkTarget, dstPath); err != nil && !os.IsExist(err) {
				return fmt.Errorf("create symlink %s: %w", dstPath, err)
			}
			continue
		}

		if info.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func prepareFS(cfg *Config) (string, error) {
	var envVarName string = ""
	switch cfg.EnvType {
	case vars.GO:
		envVarName = "GO_ENV_SRC"
	case vars.PYTHON:
		envVarName = "PYTHON_ENV_SRC"
	case vars.JAVA:
		envVarName = "JAVA_ENV_SRC"
	}

	if envVarName == "" {
		return "", fmt.Errorf("unknown env type: %s", cfg.EnvType)
	}

	srcEnv := os.Getenv(envVarName)
	if srcEnv == "" {
		return "", fmt.Errorf("%s is not set", envVarName)
	}
	if _, err := os.Stat(srcEnv); err != nil {
		return "", fmt.Errorf("env dir %s not found: %w", srcEnv, err)
	}

	id := make([]byte, 8)
	rand.Read(id)
	rootfs := filepath.Join(os.TempDir(), fmt.Sprintf("invoke-%x", id))

	if err := copyDir(srcEnv, rootfs); err != nil {
		return "", fmt.Errorf("copy env: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(rootfs, "oldroot"), 0o755); err != nil {
		return "", fmt.Errorf("create oldroot: %w", err)
	}

	filename := filepath.Base(cfg.CodeSource)
	dstCode := filepath.Join(rootfs, filename)
	if err := copyFile(cfg.CodeSource, dstCode); err != nil {
		return "", fmt.Errorf("copy user code: %w", err)
	}

	cfg.CodeSource = "/" + filename

	return rootfs, nil
}

func Run(cfg Config) *Result {
	rootfs, err := prepareFS(&cfg)
	if err != nil {
		return nil
	}

	p := params.Params{
		EnvType:    cfg.EnvType,
		CodeSource: cfg.CodeSource,
		RootFS:     rootfs,

		Flags: uintptr(syscall.CLONE_NEWUSER | syscall.CLONE_NEWUTS | syscall.CLONE_NEWNS | syscall.CLONE_NEWPID | syscall.CLONE_NEWIPC | syscall.CLONE_NEWCGROUP | syscall.CLONE_NEWNET | syscall.SIGCHLD),

		MemoryLimit: cfg.MemoryLimit,
		CPUQuota:    cfg.CPUQuota,
		TimeLimit:   cfg.TimeLimit,
	}

	r := ns.Start(p)
	if r == nil {
		return nil
	}

	result := &Result{
		ExitCode: r.ExitCode,
		TimedOut: r.TimedOut,
		Stdout:   r.Stdout,
	}

	return result
}
