package dremio

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceSourceMssql() *schema.Resource {
	return makeResourceSource("MSSQL", makeMssqlSourceConfig, readMssqlSource, map[string]*schema.Schema{
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
		"username": {
			Type:     schema.TypeString,
			Required: true,
		},
		"password": {
			Type:      schema.TypeString,
			Required:  true,
			Sensitive: true,
		},
		"hostname": {
			Type:     schema.TypeString,
			Required: true,
		},
		"port": {
			Type:     schema.TypeString,
			Required: true,
		},
		"authentication_type": {
			Type:     schema.TypeString,
			Required: true,
		},
		"fetch_size": {
			Type:     schema.TypeInt,
			Optional: true,
			Default:  0,
		},
		"database": {
			Type:     schema.TypeString,
			Optional: true,
			Default:  "",
		},
		"show_only_connection_database": {
			Type:     schema.TypeBool,
			Optional: true,
			Default:  true,
		},
	})
}

func makeMssqlSourceConfig(d *schema.ResourceData) interface{} {
	return map[string]interface{}{
		"username":                   d.Get("username").(string),
		"password":                   d.Get("password").(string),
		"hostname":                   d.Get("hostname").(string),
		"port":                       d.Get("port").(string),
		"authenticationType":         d.Get("authentication_type").(string),
		"fetchSize":                  d.Get("fetch_size").(int),
		"database":                   d.Get("database").(string),
		"showOnlyConnectionDatabase": d.Get("show_only_connection_database").(bool),
	}
}

func readMssqlSource(source *dapi.Source, d *schema.ResourceData) diag.Diagnostics {
	if source.Type != "MSSQL" {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Detail:   fmt.Sprintf("Expected MSSQL type, found %v", source.Type),
			},
		}
	}
	config := source.Config.(map[string]interface{})
	if err := d.Set("username", config["username"].(string)); err != nil {
		return diag.FromErr(err)
	}
	// Ignore password as Dremio returns this as $DREMIO_EXISTING_VALUE$

	if err := d.Set("hostname", config["hostname"].(string)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("port", config["port"].(string)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("authentication_type", config["authenticationType"].(string)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("fetch_size", int(config["fetchSize"].(float64))); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("database", config["database"].(string)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("show_only_connection_database", config["showOnlyConnectionDatabase"].(bool)); err != nil {
		return diag.FromErr(err)
	}
	return diag.Diagnostics{}
}
