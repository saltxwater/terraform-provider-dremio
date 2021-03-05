package dremio

import (
	"context"
	"errors"
	"log"

	dapi "github.com/saltxwater/go-dremio-api-client"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceCatalog() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceCatalogRead,
		Schema: map[string]*schema.Schema{
			"absolute_path": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"relative_path": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"parent_id": {
				Type:     schema.TypeString,
				Optional: true,
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

func dataSourceCatalogRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	absPath, err := getAbsolutePath(client, d)
	if err != nil {
		return diag.FromErr(err)
	}
	log.Printf("Absolute path is %v", absPath)
	target, err := client.GetCatalogEntityByPath(absPath)
	if err != nil {
		return diag.FromErr(err)
	}
	log.Printf("Target is %v", target)

	if err := d.Set("path", target.Path); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("query_path", getQueryPath(target.Path)); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("type", target.EntityType); err != nil {
		return diag.FromErr(err)
	}

	// always run
	d.SetId(target.Id)

	return diags
}

func getAbsolutePath(client *dapi.Client, d *schema.ResourceData) ([]string, error) {
	absPath := interfaceListToStringList(d.Get("absolute_path").([]interface{}))
	if len(absPath) > 0 {
		return absPath, nil
	}
	parentId := d.Get("parent_id").(string)
	if parentId != "" {
		log.Printf("Using catalog entry with id '%s' as root", parentId)
		parent, err := client.GetCatalogEntityById(parentId)
		if err != nil {
			return nil, err
		}
		relPath := interfaceListToStringList(d.Get("relative_path").([]interface{}))
		return append(parent.Path, relPath...), nil
	}
	return nil, errors.New("Expected absolute_path or parent_id to be set")
}
