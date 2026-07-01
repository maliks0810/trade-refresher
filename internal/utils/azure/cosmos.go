package azure

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

// NewCosmos executes an instantiation of a private structure that implements the Cosmos
// interface.
//	- url is the URL address of the Azure CosmosDB instance
//	- databaseID is the specific database ID within in Azure CosmosDB instance
//	- containerID is the specific container ID within the database of the Azure CosmosDB instance
//	- partitionKey is the partition key of the container within Azure CosmosDB instance
// 	- credentials is the Azure CosmoDB client credentials
func NewCosmos(
	url 				string, 
	databaseID 			string, 
	containerID 		string, 
	partitionKey 		string,
	credentials			azcosmos.KeyCredential) (Cosmos, bool) {
	if url == "" || databaseID == "" || containerID == ""  ||   partitionKey == "" {
		return nil, false
	}

	return &cosmos{
		url: url,
		database: databaseID,
		container: containerID,
		pk: partitionKey,
		creds: credentials,
	}, true
}

type cosmos struct {
	url 			string
	database 		string
	container 		string
	pk 				string
	creds			azcosmos.KeyCredential
}

type reader interface {
	// Iterator executes a query to return a collection of documents from the Azure CosmosDB container
	//
	// Parameters:
	//
	// string: The SQL statement to query records in Azure CosmosDB 
	Iterator(string) (*runtime.Pager[azcosmos.QueryItemsResponse], error)
	// Read executes a call to return a specific document from the Azure CosmosDB container
	//
	// Parameters:
	//
	// - id : string The unique ID of the record to retrieve from Azure CosmosDB
	//
	// - ctx : context.Context The executing context of the read request
	Read(string, context.Context) (azcosmos.ItemResponse, error)
}

type adder interface {
	// Create executes a call to insert a new document into the Azure CosmosDB container
	//
	// Parameters:
	//
	// - item : []byte The item to insert into in Azure CosmosDB
	//
	// - ctx : context.Context The executing context of the read request
	Create([]byte, context.Context) (azcosmos.ItemResponse, error)
}

type replacer interface {
	// Replace executes a call to replace a pre-existing document in the Azure CosmosDB container
	//
	// Parameters:
	//
	// - id : string The unique ID of the record to replace in Azure CosmosDB
	//
	// - item : []byte The item to insert into Azure CosmosDB
	//
	// - ctx : context.Context The executing context of the read request
	Replace(string, []byte, context.Context) (azcosmos.ItemResponse, error)
}

type deleter interface {
	// Delete executes a call to delete a pre-existing document in the Azure CosmosDB container
	//
	// Parameters:
	//
	// - id : string The unique ID of the record to delete from Azure CosmosDB
	//
	// - ctx : context.Context The executing context of the read request
	Delete(string, context.Context) (azcosmos.ItemResponse, error)
}

type Cosmos interface {
	reader
	adder
	replacer
	deleter
}

// Iterator implements the reader Iterator method
func (c *cosmos) Iterator(sql string) (*runtime.Pager[azcosmos.QueryItemsResponse], error) {
	client, err := getCosmosContainerClient(c);
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate an Azure CosmosDB container client: %w", err)
	}

	pk := azcosmos.NewPartitionKeyString(c.pk)

	return client.NewQueryItemsPager(sql, pk, nil), nil
}

// Read implements the reader Read method
func (c *cosmos) Read(id string, ctx context.Context) (azcosmos.ItemResponse, error) {
	client, err := getCosmosContainerClient(c);
	if err != nil {
		return azcosmos.ItemResponse{}, fmt.Errorf("unable to instantiate an Azure CosmosDB container client: %w", err)
	}

	pk := azcosmos.NewPartitionKeyString(c.pk)

	return client.ReadItem(ctx, pk, id, nil)
}

// Create implements the adder Create method
func (c *cosmos) Create(item []byte, ctx context.Context) (azcosmos.ItemResponse, error) {
	client, err := getCosmosContainerClient(c)
	if err != nil {
		return azcosmos.ItemResponse{}, fmt.Errorf("unable to instantiate an Azure CosmosDB container client: %w", err)
	}

	pk := azcosmos.NewPartitionKeyString(c.pk)

	return client.CreateItem(ctx, pk, item, nil)
}

// Replace implements the replacer Replace method
func (c *cosmos) Replace(id string, item []byte, ctx context.Context) (azcosmos.ItemResponse, error) {
	client, err := getCosmosContainerClient(c)
	if err != nil {
		return azcosmos.ItemResponse{}, fmt.Errorf("unable to instantiate an Azure CosmosDB container client: %w", err)
	}

	pk := azcosmos.NewPartitionKeyString(c.pk)

	return client.ReplaceItem(ctx, pk, id, item, nil)
}

// Delete implements the deleter Delete method
func (c *cosmos) Delete(id string, ctx context.Context) (azcosmos.ItemResponse, error) {
	client, err := getCosmosContainerClient(c)
	if err != nil {
		return azcosmos.ItemResponse{},fmt.Errorf("unable to instantiate an Azure CosmosDB container client: %w", err)
	}

	pk := azcosmos.NewPartitionKeyString(c.pk)

	return client.DeleteItem(ctx, pk, id, nil)
}

func getCosmosContainerClient(c *cosmos) (*azcosmos.ContainerClient, error) {
	if c.url == "" {
		return nil, errors.New("invalid Cosmos URL supplied")
	}
	if c.database == "" {
		return nil, errors.New("invalid database id supplied")
	}
	if c.container == "" {
		return nil, errors.New("invalid container id supplied")
	}

	cosmosClient, err := azcosmos.NewClientWithKey(c.url, c.creds, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate an Azure CosmosDB client: %w", err)
	}

	dbClient, err := getCosmosDatabase(cosmosClient, c.database)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate an Azure CosmosDB database client: %w", err)
	}

	return dbClient.NewContainer(c.container)
}

func getCosmosDatabase(client *azcosmos.Client, database string) (*azcosmos.DatabaseClient, error) {
	if client == nil {
		return nil, errors.New("invalid Azure CosmosDB client supplied - azcosmos.Client is nil")
	}

	return client.NewDatabase(database)
}
