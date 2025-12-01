package config

import (
	"strings"

	"github.com/fatih/structs"
	"github.com/jeremywohl/flatten"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

func ParseConfig[T interface{}](configFilePaths []string) (*T, error) {
	for _, v := range configFilePaths {
		viper.AddConfigPath(v)
	}
	viper.SetConfigName("config.yaml")
	viper.SetConfigType("yaml")

	err := bindAllConfigKeys[T]()
	if err != nil {
		return nil, err
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	err = viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	var c *T
	err = viper.Unmarshal(&c)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to decode into struct")
	}

	return c, nil
}

// Workaround for major viper issue with env variables, documented here
// https://github.com/spf13/viper/issues/761
func bindAllConfigKeys[T interface{}]() error {
	var cd T
	// Transform config struct to map
	confMap := structs.Map(cd)

	// Flatten nested conf map
	flat, err := flatten.Flatten(confMap, "", flatten.DotStyle)
	if err != nil {
		return errors.Wrap(err, "Unable to flatten config")
	}

	// Bind each conf fields to environment vars
	for key := range flat {
		err := viper.BindEnv(key)
		if err != nil {
			return errors.Wrapf(err, "Unable to bind env var: %s", key)
		}
	}
	return nil
}
