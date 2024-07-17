package configs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

const (
	configFileName = `birdwatcher.yaml`
)

var (
	errConfigPathNotExist = errors.New("config path not exist")
	errConfigPathIsFile   = errors.New("config path is file")
)

// Config stores birdwatcher config items.
type Config struct {
	// birdwatcher configuration folder path
	// default $PWD/.bw_config
	ConfigPath string `yaml:"-"`
	// backup workspace path, default $PWD/bw_workspace
	WorkspacePath string `yaml:"WorkspacePath"`
}

func GetDefaultConfigPath() string {
	return path.Join(GetDefaultWorkspacePath(), ".bw_config")
}

func GetDefaultDebugPath() string {
	return path.Join(GetDefaultWorkspacePath(), "bw_debug.log")
}

func GetDefaultWorkspacePath() string {
	userDir, err := os.UserHomeDir()
	if err != nil {
		return "bw_workspace"
	}
	return path.Join(userDir, "bw_workspace")
}

func (c *Config) load() error {
	err := c.checkConfigPath()
	if err != nil {
		return err
	}

	f, err := os.Open(c.getConfigPath())
	if err != nil {
		return err
	}
	defer f.Close()
	bs, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(bs, c)
}

func (c *Config) getConfigPath() string {
	return path.Join(c.ConfigPath, configFileName)
}

// checkConfigPath exists and is a directory.
func (c *Config) checkConfigPath() error {
	info, err := os.Stat(c.ConfigPath)
	if err != nil {
		// not exist, return specified type to handle
		if os.IsNotExist(err) {
			return errConfigPathNotExist
		}
		return err
	}
	if !info.IsDir() {
		fmt.Printf("%s is not a directory\n", c.ConfigPath)
		return fmt.Errorf("%w(%s)", errConfigPathIsFile, configFileName)
	}

	return nil
}

func (c *Config) createDefault() error {
	err := os.MkdirAll(c.ConfigPath, os.ModePerm)
	if err != nil {
		return err
	}

	file, err := os.Create(c.getConfigPath())
	if err != nil {
		return err
	}
	defer file.Close()

	// setup default value
	c.WorkspacePath = GetDefaultWorkspacePath()

	bs, err := yaml.Marshal(c)
	if err != nil {
		fmt.Println("failed to marshal config", err.Error())
		return err
	}

	file.Write(bs)
	return nil
}

func (c *Config) setupWorkspaceFolder() {
	// try best to setup
	err := os.MkdirAll(c.WorkspacePath, os.ModePerm)
	if err != nil {
		fmt.Println("failed to set workspace folder", err.Error())
	}
}

func NewConfig() (*Config, error) {
	config := &Config{
		ConfigPath: GetDefaultConfigPath(),
	}
	err := config.createDefault()

	config.setupWorkspaceFolder()

	return config, err
}
