package dremio

import (
	"context"
	"strconv"
	"time"

	dapi "github.com/saltxwater/go-dremio-api-client"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceSummary() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceSummaryRead,
		Schema: map[string]*schema.Schema{
			"summary": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"type": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
	}
}

func dataSourceSummaryRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	summary, err := client.GetRootCatalogSummary()
	if err != nil {
		return diag.FromErr(err)
	}

	fs := make([]map[string]interface{}, 0)
	for _, x := range summary {
		fs = append(fs, map[string]interface{}{
			"id":   x.Id,
			"type": x.Type,
		})
	}

	if err := d.Set("summary", fs); err != nil {
		return diag.FromErr(err)
	}

	// always run
	d.SetId(strconv.FormatInt(time.Now().Unix(), 10))

	return diags
}
