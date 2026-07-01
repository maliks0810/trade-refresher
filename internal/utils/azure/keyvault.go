package azure

import (
	"context"
	"errors"
	"fmt"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

// NewVault executes an instantiation of a private vault struct that implements the Vault interface
//
// Parameters:
//
//	- url : string Validated URL of the desired Azure KeyVault instance
//
//	- useWorkloadIdentity : bool Indicates whether authentication should use the Workload Identity credentials
func NewVault(url string, useWorkloadIdentity bool) Vault {
	return &vault{
		url: url,
		workload: useWorkloadIdentity,
	}
}

type vault struct {
	url			string
	workload	bool
}

type getter interface {
	// Get executes a call to retrieve a single value from the Azure KeyVault using the provided key
	//
	// Parameters:
	//
	// - key : string The unique key of the item to retrieve from Azure KeyVault
	Get(string) (string, error)
	// GetMany executes a call to retrieve multiple values from the Azure KeyVault using the provided key slice
	//
	// Parameters:
	//
	// keys : []string A map of unique keys of items to retrieve from Azure KeyVault
	GetMany([]string) (map[string]string, error)
}

// Vault interface provides methods to retrieve secure values from the Azure KeyVault.  Access to the
// Azure KeyVault is facilitated by authenticating with either the Azure CLI when running locally, or
// with the Workload Identity configured for the Azure Kubernetes Pod deployment
type Vault interface {
	getter
}

// Get implements the getter Get method
func (v vault) Get(key string) (string, error) {
	if key == "" {
		return "", errors.New("keyvault.go: GetSecret - Invalid key supplied - key cannot be empty string")
	}

	client, err := getKeyVaultClient(v.url, v.workload)
	if err != nil {
		return "", err
	}

	response, err := client.GetSecret(context.TODO(), key, "", nil)
	if err != nil {
		return "", err
	}

	return *response.Value, nil
}

// GetMany implements the getter GetMany method
func (v vault) GetMany(keys []string) (map[string]string, error) {
	if keys == nil {
		return nil, errors.New("keyvault.go: GetSecrets - Invalid collection of keys supplied - cannot be nil")
	}
	if len(keys) == 0 {
		return nil, errors.New("keyvault.go: GetSecrets - Invalid collection of keys supplied - cannot be empty")
	}

	client, err := getKeyVaultClient(v.url, v.workload)
	if err != nil {
		return nil, err
	}

	results := make(map[string]string)
	for _, s := range keys {
		response, err := client.GetSecret(context.TODO(), s, "", nil)
		if err != nil {
			return nil, err
		}

		results[s] = *response.Value
	}

	return results, nil
}

func getKeyVaultClient(url string, workload bool) (*azsecrets.Client, error) {
	if url == "=" {
		return nil, errors.New("invalid Azure Key Vault URL supplied")
	}
	
	azlog.SetListener(func(event azlog.Event, s string) {
		fmt.Println("azidentity: ", s)
	})
	azlog.SetEvents(azidentity.EventAuthentication)

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

func getKeyVaultClientViaCli(url string) (*azsecrets.Client, error)  {
	credentials, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create an Azure CLI Identity: %w", err)
	}

	return azsecrets.NewClient(url, credentials, nil)
}