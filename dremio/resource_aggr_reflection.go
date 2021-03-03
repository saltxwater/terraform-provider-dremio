package dremio

import (
	"context"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceAggregationReflection() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceAggregationReflectionCreate,
		ReadContext:   resourceAggregationReflectionRead,
		UpdateContext: resourceAggregationReflectionUpdate,
		DeleteContext: resourceAggregationReflectionDelete,
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
			"dimension_fields": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"granularity": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
			"measure_fields": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"measure": {
							Type:     schema.TypeSet,
							Required: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
					},
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

func getDimensionFields(d *schema.ResourceData) []dapi.ReflectionFieldWithGranularity {
	itemsRaw := d.Get("dimension_fields").([]interface{})
	items := make([]dapi.ReflectionFieldWithGranularity, len(itemsRaw))
	for i, raw := range itemsRaw {
		rawMap := raw.(map[string]interface{})
		items[i] = dapi.ReflectionFieldWithGranularity{
			ReflectionField: dapi.ReflectionField{
				Name: rawMap["name"].(string),
			},
			Granularity: rawMap["granularity"].(string),
		}
	}
	return items
}

func setDimensionFields(d *schema.ResourceData, fields []dapi.ReflectionFieldWithGranularity) error {
	items := make([]map[string]string, len(fields))
	for i, raw := range fields {
		items[i] = map[string]string{
			"name":        raw.Name,
			"granularity": raw.Granularity,
		}
	}
	return d.Set("dimension_fields", items)
}

func getMeasureFields(d *schema.ResourceData) []dapi.ReflectionMeasureField {
	itemsRaw := d.Get("measure_fields").([]interface{})
	items := make([]dapi.ReflectionMeasureField, len(itemsRaw))
	for i, raw := range itemsRaw {
		rawMap := raw.(map[string]interface{})
		items[i] = dapi.ReflectionMeasureField{
			ReflectionField: dapi.ReflectionField{
				Name: rawMap["name"].(string),
			},
			MeasureTypeList: interfaceListToStringList(rawMap["measure"].(*schema.Set).List()),
		}
	}
	return items
}

func setMeasureFields(d *schema.ResourceData, fields []dapi.ReflectionMeasureField) error {
	items := make([]map[string]interface{}, len(fields))
	for i, raw := range fields {
		items[i] = map[string]interface{}{
			"name":    raw.Name,
			"measure": raw.MeasureTypeList,
		}
	}
	return d.Set("measure_fields", items)
}

func resourceAggregationReflectionCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	datasetId := d.Get("dataset_id").(string)
	name := d.Get("name").(string)
	enabled := d.Get("enabled").(bool)
	dimensionFields := getDimensionFields(d)
	measureFields := getMeasureFields(d)
	distributionFields := interfaceListToReflectionFieldList(d.Get("distribution_fields").([]interface{}))
	partitionFields := interfaceListToReflectionFieldList(d.Get("partition_fields").([]interface{}))
	sortFields := interfaceListToReflectionFieldList(d.Get("sort_fields").([]interface{}))
	partitionDistributionStrategy := d.Get("partition_distribution_strategy").(string)

	res, err := c.NewAggregationReflection(datasetId, &dapi.AggregationReflectionSpec{
		Name:                          name,
		Enabled:                       enabled,
		DimensionFields:               dimensionFields,
		MeasureFields:                 measureFields,
		DistributionFields:            distributionFields,
		PartitionFields:               partitionFields,
		SortFields:                    sortFields,
		PartitionDistributionStrategy: partitionDistributionStrategy,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(res.Id)

	resourceAggregationReflectionRead(ctx, d, m)

	return diags
}

func resourceAggregationReflectionRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	resId := d.Id()

	res, err := c.GetAggregationReflection(resId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("name", res.Name)
	d.Set("enabled", res.Enabled)
	if err := setDimensionFields(d, res.DimensionFields); err != nil {
		return diag.FromErr(err)
	}
	if err := setMeasureFields(d, res.MeasureFields); err != nil {
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

func resourceAggregationReflectionUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	resId := d.Id()

	name := d.Get("name").(string)
	enabled := d.Get("enabled").(bool)
	dimensionFields := getDimensionFields(d)
	measureFields := getMeasureFields(d)
	distributionFields := interfaceListToReflectionFieldList(d.Get("distribution_fields").([]interface{}))
	partitionFields := interfaceListToReflectionFieldList(d.Get("partition_fields").([]interface{}))
	sortFields := interfaceListToReflectionFieldList(d.Get("sort_fields").([]interface{}))
	partitionDistributionStrategy := d.Get("partition_distribution_strategy").(string)

	_, err := c.UpdateAggregationReflection(resId, &dapi.AggregationReflectionSpec{
		Name:                          name,
		Enabled:                       enabled,
		DimensionFields:               dimensionFields,
		MeasureFields:                 measureFields,
		DistributionFields:            distributionFields,
		PartitionFields:               partitionFields,
		SortFields:                    sortFields,
		PartitionDistributionStrategy: partitionDistributionStrategy,
	})
	if err != nil {
		return diag.FromErr(err)
	}
	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourceAggregationReflectionRead(ctx, d, m)
}

func resourceAggregationReflectionDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
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
