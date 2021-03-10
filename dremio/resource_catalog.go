package dremio

import (
	"context"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceCatalog() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceCatalogCreate,
		ReadContext:   resourceCatalogRead,
		DeleteContext: resourceCatalogDelete,
		Schema: map[string]*schema.Schema{
			"absolute_path": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"relative_path": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"parent_id": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"path": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"query_path": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"type": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceCatalogCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	absPath, err := getAbsolutePath(client, d.Get("parent_id").(string), d.Get("relative_path").([]interface{}))
	if err != nil {
		return diag.FromErr(err)
	}
	log.Printf("Absolute path is %v", absPath)
	target, err := client.GetCatalogEntityByPath(absPath)
	if err != nil {
		return diag.FromErr(err)
	}
	log.Printf("Target is %v", target)

	d.SetId(target.Id)

	resourceCatalogRead(ctx, d, m)

	return diags
}

func resourceCatalogRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	resId := d.Id()

	target, err := c.GetCatalogEntityById(resId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("path", target.Path); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("query_path", getQueryPath(target.Path)); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("type", target.EntityType); err != nil {
		return diag.FromErr(err)
	}
	return diags
}

func resourceCatalogDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	d.SetId("")
	return diags
}
