package dremio

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func makePhysicalDatasetSchema(s map[string]*schema.Schema) map[string]*schema.Schema {
	s["source_id"] = &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	}
	s["relative_path"] = &schema.Schema{
		Type:     schema.TypeList,
		Required: true,
		ForceNew: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	}
	s["acc_refresh_period_ms"] = &schema.Schema{
		Type:     schema.TypeInt,
		Optional: true,
		Default:  10800000,
	}
	s["acc_grace_period_ms"] = &schema.Schema{
		Type:     schema.TypeInt,
		Optional: true,
		Default:  32400000,
	}
	s["acc_method"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
		Default:  "FULL",
		ValidateFunc: func(val interface{}, key string) (warns []string, errs []error) {
			v := val.(string)
			if v == "FULL" || v == "INCREMENTAL" {
				return
			}
			errs = append(errs, fmt.Errorf("%s must be 'FULL' or 'INCREMENTAL', got: %s", key, v))
			return
		},
	}
	s["acc_refresh_field"] = &schema.Schema{
		Type:     schema.TypeString,
		Optional: true,
	}
	return makeDatasetSchema(s)
}

func makeDatasetSchema(s map[string]*schema.Schema) map[string]*schema.Schema {
	s["fields"] = &schema.Schema{
		Type:     schema.TypeList,
		Computed: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"name": {
					Type:     schema.TypeString,
					Computed: true,
				},
				"type": {
					Type:     schema.TypeString,
					Computed: true,
				},
			},
		},
	}
	s["path"] = &schema.Schema{
		Type:     schema.TypeList,
		Computed: true,
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
	}
	s["query_path"] = &schema.Schema{
		Type:     schema.TypeString,
		Computed: true,
	}
	return s
}

func readPhysicalDatasetRefreshPolicy(d *schema.ResourceData, pds *dapi.PhysicalDataset) error {
	acc := pds.AccelerationRefreshPolicy
	if acc == nil {
		if err := d.Set("acc_refresh_period_ms", 0); err != nil {
			return err
		}
		if err := d.Set("acc_grace_period_ms", 0); err != nil {
			return err
		}
		if err := d.Set("acc_method", ""); err != nil {
			return err
		}
		if err := d.Set("acc_refresh_field", ""); err != nil {
			return err
		}
	} else {
		if err := d.Set("acc_refresh_period_ms", acc.RefreshPeriodMs); err != nil {
			return err
		}
		if err := d.Set("acc_grace_period_ms", acc.GracePeriodMs); err != nil {
			return err
		}
		if err := d.Set("acc_method", acc.Method); err != nil {
			return err
		}
		if err := d.Set("acc_refresh_field", acc.RefreshField); err != nil {
			return err
		}
	}
	return nil
}

func readDatasetCommon(d *schema.ResourceData, ds *dapi.Dataset) error {
	fields := make([]map[string]string, len(ds.Fields))
	for i, field := range ds.Fields {
		fields[i] = map[string]string{
			"name": field.Name,
			"type": field.Type.Name,
		}
	}
	if err := d.Set("fields", fields); err != nil {
		return err
	}

	if err := d.Set("path", ds.Path); err != nil {
		return err
	}

	if err := d.Set("query_path", getQueryPath(ds.Path)); err != nil {
		return err
	}
	return nil
}

func readPhysicalDatasetCommon(d *schema.ResourceData, pds *dapi.PhysicalDataset) error {
	if err := readPhysicalDatasetRefreshPolicy(d, pds); err != nil {
		return err
	}
	return readDatasetCommon(d, &pds.Dataset)
}

func getDatasetAccelerationRefreshPolicy(d *schema.ResourceData) *dapi.DatasetAccelerationRefreshPolicy {
	return &dapi.DatasetAccelerationRefreshPolicy{
		RefreshPeriodMs: d.Get("acc_refresh_period_ms").(int),
		GracePeriodMs:   d.Get("acc_grace_period_ms").(int),
		Method:          d.Get("acc_method").(string),
		RefreshField:    d.Get("acc_refresh_field").(string),
	}
}

func getPhysicalDatasetAbsolutePath(c *dapi.Client, d *schema.ResourceData) ([]string, error) {
	return getAbsolutePath(c, d.Get("source_id").(string), d.Get("relative_path").([]interface{}))
}
