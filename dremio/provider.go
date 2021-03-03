package dremio

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

// Provider -
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"dremio_url": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("DREMIO_URL", nil),
			},
			"api_key": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("DREMIO_API_KEY", nil),
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("DREMIO_USERNAME", nil),
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("DREMIO_PASSWORD", nil),
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"dremio_folder":           resourceFolder(),
			"dremio_space":            resourceSpace(),
			"dremio_source_mssql":     resourceSourceMssql(),
			"dremio_source_nas":       resourceSourceNas(),
			"dremio_virtual_dataset":  resourceVirtualDataset(),
			"dremio_physical_dataset": resourcePhysicalDataset(),
			"dremio_raw_reflection":   resourceRawReflection(),
			"dremio_aggr_reflection":  resourceAggregationReflection(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"dremio_summary": dataSourceSummary(),
		},
		ConfigureContextFunc: providerConfigure,
	}
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	username := d.Get("username").(string)
	password := d.Get("password").(string)
	apiKey := d.Get("api_key").(string)
	baseUrl := d.Get("dremio_url").(string)

	var diags diag.Diagnostics

	config := dapi.Config{
		ApiKey:   apiKey,
		Username: username,
		Password: password,
	}
	client, err := dapi.NewClient(baseUrl, config)
	if err != nil {
		return nil, diag.FromErr(err)
	}
	return client, diags
}

func interfaceListToStringList(itemsRaw []interface{}) []string {
	items := make([]string, len(itemsRaw))
	for i, raw := range itemsRaw {
		items[i] = raw.(string)
	}
	return items
}
