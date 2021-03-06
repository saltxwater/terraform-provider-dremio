package dremio

import (
	"context"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceVirtualDataset() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceVirtualDatasetCreate,
		ReadContext:   resourceVirtualDatasetRead,
		UpdateContext: resourceVirtualDatasetUpdate,
		DeleteContext: resourceVirtualDatasetDelete,
		Schema: makeDatasetSchema(map[string]*schema.Schema{
			"parent_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"sql": {
				Type:     schema.TypeString,
				Required: true,
			},
			"sql_context": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
		),
	}
}

func resourceVirtualDatasetCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	parent, err := c.GetCatalogEntityById(d.Get("parent_id").(string))
	if err != nil {
		return diag.FromErr(err)
	}

	inputPath := append(parent.Path, d.Get("name").(string))

	inputSCtx := d.Get("sql_context").([]interface{})
	sCtx := make([]string, len(inputSCtx))
	for i, elem := range inputSCtx {
		sCtx[i] = elem.(string)
	}

	vds, err := c.NewVirtualDataset(&dapi.NewVirtualDatasetSpec{
		Path:       inputPath,
		Sql:        d.Get("sql").(string),
		SqlContext: sCtx,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(vds.Id)

	resourceVirtualDatasetRead(ctx, d, m)

	return diags
}

func resourceVirtualDatasetRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	vdsId := d.Id()

	vds, err := c.GetVirtualDataset(vdsId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("sql", vds.Sql); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("sql_context", vds.SqlContext); err != nil {
		return diag.FromErr(err)
	}

	if err := readDatasetCommon(d, &vds.Dataset); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceVirtualDatasetUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	sourceId := d.Id()

	inputSCtx := d.Get("sql_context").([]interface{})
	sCtx := make([]string, len(inputSCtx))
	for i, elem := range inputSCtx {
		sCtx[i] = elem.(string)
	}

	_, err := c.UpdateVirtualDataset(sourceId, &dapi.UpdateVirtualDatasetSpec{
		Sql:        d.Get("sql").(string),
		SqlContext: sCtx,
	})
	if err != nil {
		return diag.FromErr(err)
	}
	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourceVirtualDatasetRead(ctx, d, m)
}

func resourceVirtualDatasetDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	vdsId := d.Id()

	err := c.DeleteCatalogItem(vdsId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}
