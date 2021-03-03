package dremio

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceSourceNas() *schema.Resource {
	return makeResourceSource("NAS", makeNasSourceConfig, readNasSource, map[string]*schema.Schema{
		"name": {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
		},
		"description": {
			Type:     schema.TypeString,
			Optional: true,
			Default:  "",
		},
		"path": {
			Type:     schema.TypeString,
			Required: true,
		},
	})
}

func makeNasSourceConfig(d *schema.ResourceData) interface{} {
	return map[string]interface{}{
		"path": d.Get("path").(string),
	}
}

func readNasSource(source *dapi.Source, d *schema.ResourceData) diag.Diagnostics {
	if source.Type != "NAS" {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Detail:   fmt.Sprintf("Expected NAS type, found %v", source.Type),
			},
		}
	}
	config := source.Config.(map[string]interface{})
	if err := d.Set("path", config["path"].(string)); err != nil {
		return diag.FromErr(err)
	}
	return diag.Diagnostics{}
}
