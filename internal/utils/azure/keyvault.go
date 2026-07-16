package azure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

const keyVaultRequestTimeout = 15 * time.Second

func NewVault(url string, useWorkloadIdentity bool) Vault {
	return &vault{
		url:      url,
		workload: useWorkloadIdentity,
	}
}

type vault struct {
	url      string
	workload bool

	once      sync.Once
	client    *azsecrets.Client
	clientErr error
}

type getter interface {
	Get(string) (string, error)
	GetMany([]string) (map[string]string, error)
}

type Vault interface {
	getter
}

func (v *vault) Get(key string) (string, error) {
	if key == "" {
		return "", errors.New("keyvault.go: GetSecret - Invalid key supplied - key cannot be empty string")
	}

	client, err := v.clientForVault()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), keyVaultRequestTimeout)
	defer cancel()

	response, err := client.GetSecret(ctx, key, "", nil)
	if err != nil {
		return "", err
	}
	if response.Value == nil {
		return "", fmt.Errorf("key vault secret %q has no value", key)
	}

	return *response.Value, nil
}

func (v *vault) GetMany(keys []string) (map[string]string, error) {
	if keys == nil {
		return nil, errors.New("keyvault.go: GetSecrets - Invalid collection of keys supplied - cannot be nil")
	}
	if len(keys) == 0 {
		return nil, errors.New("keyvault.go: GetSecrets - Invalid collection of keys supplied - cannot be empty")
	}

	client, err := v.clientForVault()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), keyVaultRequestTimeout)
	defer cancel()

	results := make(map[string]string)
	for _, s := range keys {
		response, err := client.GetSecret(ctx, s, "", nil)
		if err != nil {
			return nil, err
		}
		if response.Value == nil {
			return nil, fmt.Errorf("key vault secret %q has no value", s)
		}

		results[s] = *response.Value
	}

	return results, nil
}

func (v *vault) clientForVault() (*azsecrets.Client, error) {
	v.once.Do(func() {
		v.client, v.clientErr = getKeyVaultClient(v.url, v.workload)
	})
	return v.client, v.clientErr
}

func getKeyVaultClient(url string, workload bool) (*azsecrets.Client, error) {
	if url == "=" {
		return nil, errors.New("invalid Azure Key Vault URL supplied")
	}

	if workload {
		return getKeyVaultClientViaWorkload(url)
	}

	return getKeyVaultClientViaCli(url)
}

func getKeyVaultClientViaWorkload(url string) (*azsecrets.Client, error) {
	credentials, err := azidentity.NewWorkloadIdentityCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create an Azure Workload Identity: %w", err)
	}

	return azsecrets.NewClient(url, credentials, nil)
}

func getKeyVaultClientViaCli(url string) (*azsecrets.Client, error) {
	credentials, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create an Azure CLI Identity: %w", err)
	}

	return azsecrets.NewClient(url, credentials, nil)
}
