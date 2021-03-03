package dremio

import (
	"context"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceRawReflection() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceRawReflectionCreate,
		ReadContext:   resourceRawReflectionRead,
		UpdateContext: resourceRawReflectionUpdate,
		DeleteContext: resourceRawReflectionDelete,
		Schema: map[string]*schema.Schema{
			"dataset_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"display_fields": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"distribution_fields": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"partition_fields": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"sort_fields": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"partition_distribution_strategy": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "CONSOLIDATED",
			},
		},
	}
}

func reflectionFieldListToStringList(itemsRaw []dapi.ReflectionField) []string {
	items := make([]string, len(itemsRaw))
	for i, raw := range itemsRaw {
		items[i] = raw.Name
	}
	return items
}

func interfaceListToReflectionFieldList(itemsRaw []interface{}) []dapi.ReflectionField {
	items := make([]dapi.ReflectionField, len(itemsRaw))
	for i, raw := range itemsRaw {
		items[i] = dapi.ReflectionField{
			Name: raw.(string),
		}
	}
	return items
}

func resourceRawReflectionCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	datasetId := d.Get("dataset_id").(string)
	name := d.Get("name").(string)
	enabled := d.Get("enabled").(bool)
	displayFields := interfaceListToReflectionFieldList(d.Get("display_fields").([]interface{}))
	distributionFields := interfaceListToReflectionFieldList(d.Get("distribution_fields").([]interface{}))
	partitionFields := interfaceListToReflectionFieldList(d.Get("partition_fields").([]interface{}))
	sortFields := interfaceListToReflectionFieldList(d.Get("sort_fields").([]interface{}))
	partitionDistributionStrategy := d.Get("partition_distribution_strategy").(string)

	res, err := c.NewRawReflection(datasetId, &dapi.RawReflectionSpec{
		Name:                          name,
		Enabled:                       enabled,
		DisplayFields:                 displayFields,
		DistributionFields:            distributionFields,
		PartitionFields:               partitionFields,
		SortFields:                    sortFields,
		PartitionDistributionStrategy: partitionDistributionStrategy,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(res.Id)

	resourceRawReflectionRead(ctx, d, m)

	return diags
}

func resourceRawReflectionRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	resId := d.Id()

	res, err := c.GetRawReflection(resId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("name", res.Name)
	d.Set("enabled", res.Enabled)
	if err := d.Set("display_fields", reflectionFieldListToStringList(res.DisplayFields)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("distribution_fields", reflectionFieldListToStringList(res.DistributionFields)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("partition_fields", reflectionFieldListToStringList(res.PartitionFields)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("sort_fields", reflectionFieldListToStringList(res.SortFields)); err != nil {
		return diag.FromErr(err)
	}
	d.Set("partition_distribution_strategy", res.PartitionDistributionStrategy)

	return diags
}

func resourceRawReflectionUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	resId := d.Id()

	name := d.Get("name").(string)
	enabled := d.Get("enabled").(bool)
	displayFields := interfaceListToReflectionFieldList(d.Get("display_fields").([]interface{}))
	distributionFields := interfaceListToReflectionFieldList(d.Get("distribution_fields").([]interface{}))
	partitionFields := interfaceListToReflectionFieldList(d.Get("partition_fields").([]interface{}))
	sortFields := interfaceListToReflectionFieldList(d.Get("sort_fields").([]interface{}))
	partitionDistributionStrategy := d.Get("partition_distribution_strategy").(string)

	_, err := c.UpdateRawReflection(resId, &dapi.RawReflectionSpec{
		Name:                          name,
		Enabled:                       enabled,
		DisplayFields:                 displayFields,
		DistributionFields:            distributionFields,
		PartitionFields:               partitionFields,
		SortFields:                    sortFields,
		PartitionDistributionStrategy: partitionDistributionStrategy,
	})
	if err != nil {
		return diag.FromErr(err)
	}
	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourceRawReflectionRead(ctx, d, m)
}

func resourceRawReflectionDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	resId := d.Id()

	err := c.DeleteReflection(resId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}
