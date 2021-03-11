package dremio

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourcePhysicalDataset() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourcePhysicalDatasetCreate,
		ReadContext:   resourcePhysicalDatasetRead,
		UpdateContext: resourcePhysicalDatasetUpdate,
		DeleteContext: resourcePhysicalDatasetDelete,
		Schema:        makePhysicalDatasetSchema(map[string]*schema.Schema{}),
	}
}

func resourcePhysicalDatasetCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	absolutePath, err := getPhysicalDatasetAbsolutePath(c, d)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("Fetching target by path: %v", absolutePath)
	original, err := c.GetCatalogEntityByPath(absolutePath)
	if err != nil {
		return diag.FromErr(err)
	}
	d.SetId(original.Id)
	return resourcePhysicalDatasetUpdate(ctx, d, m)
}

func resourcePhysicalDatasetRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	pdsId := d.Id()

	pds, err := c.GetPhysicalDataset(pdsId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := readPhysicalDatasetCommon(d, pds); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourcePhysicalDatasetUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	sourceId := d.Id()

	_, err := c.UpdatePhysicalDataset(sourceId, &dapi.UpdatePhysicalDatasetSpec{
		AccelerationRefreshPolicy: getDatasetAccelerationRefreshPolicy(d),
	})
	if err != nil {
		return diag.FromErr(err)
	}
	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourcePhysicalDatasetRead(ctx, d, m)
}

func resourcePhysicalDatasetDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	pdsId := d.Id()

	_, err := c.UpdatePhysicalDataset(pdsId, &dapi.UpdatePhysicalDatasetSpec{})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}
