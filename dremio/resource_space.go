package dremio

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceSpace() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceSpaceCreate,
		ReadContext:   resourceSpaceRead,
		DeleteContext: resourceSpaceDelete,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
		},
	}
}

func resourceSpaceCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	space, err := c.NewSpace(&dapi.NewSpaceSpec{
		Name: d.Get("name").(string),
	})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(space.Id)

	resourceSpaceRead(ctx, d, m)

	return diags
}

func resourceSpaceRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	spaceId := d.Id()

	space, err := c.GetSpace(spaceId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("name", space.Name); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceSpaceDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	spaceId := d.Id()

	err := c.DeleteCatalogItem(spaceId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}
