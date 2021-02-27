package dremio

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceFolder() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceFolderCreate,
		ReadContext:   resourceFolderRead,
		DeleteContext: resourceFolderDelete,
		Schema: map[string]*schema.Schema{
			"path": {
				Type:     schema.TypeList,
				Required: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}

func interfaceListToStringList(itemsRaw []interface{}) []string {
	items := make([]string, len(itemsRaw))
	for i, raw := range itemsRaw {
		items[i] = raw.(string)
	}
	return items
}

func resourceFolderCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	folder, err := c.NewFolder(&dapi.NewFolderSpec{
		Path: interfaceListToStringList(d.Get("path").([]interface{})),
	})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(folder.Id)

	resourceFolderRead(ctx, d, m)

	return diags
}

func resourceFolderRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	folderId := d.Id()

	folder, err := c.GetFolder(folderId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("path", folder.Path); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceOrderUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	return resourceFolderRead(ctx, d, m)
}

func resourceFolderDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	folderId := d.Id()

	err := c.DeleteCatalogItem(folderId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}
