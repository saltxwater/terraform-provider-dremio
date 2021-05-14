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
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"timestamp_date_dimension_fields": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"measure_fields_sum": {
				Type:     schema.TypeList,
				Optional: true,
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

func getDimensionFields(d *schema.ResourceData) []dapi.ReflectionFieldWithGranularity {
	dimFields := d.Get("dimension_fields").([]interface{})
	lenDimFields := len(dimFields)
	tdDimFields := d.Get("timestamp_date_dimension_fields").([]interface{})
	items := make([]dapi.ReflectionFieldWithGranularity, lenDimFields+len(tdDimFields))
	for i, name := range dimFields {
		items[i] = dapi.ReflectionFieldWithGranularity{
			ReflectionField: dapi.ReflectionField{
				Name: name.(string),
			},
			Granularity: "NORMAL",
		}
	}
	for i, name := range tdDimFields {
		items[lenDimFields+i] = dapi.ReflectionFieldWithGranularity{
			ReflectionField: dapi.ReflectionField{
				Name: name.(string),
			},
			Granularity: "DATE",
		}
	}
	return items
}

func setDimensionFields(d *schema.ResourceData, fields []dapi.ReflectionFieldWithGranularity) error {
	dimFields := make([]string, 0)
	tdDimFields := make([]string, 0)
	for _, field := range fields {
		if field.Granularity == "NORMAL" {
			dimFields = append(dimFields, field.Name)
		}
		if field.Granularity == "DATE" {
			tdDimFields = append(tdDimFields, field.Name)
		}
	}
	err := d.Set("dimension_fields", dimFields)
	if err != nil {
		return err
	}
	return d.Set("timestamp_date_dimension_fields", tdDimFields)
}

func getMeasureFields(d *schema.ResourceData) []dapi.ReflectionMeasureField {
	itemsRaw := d.Get("measure_fields_sum").([]interface{})
	items := make([]dapi.ReflectionMeasureField, len(itemsRaw))
	for i, raw := range itemsRaw {
		items[i] = dapi.ReflectionMeasureField{
			ReflectionField: dapi.ReflectionField{
				Name: raw.(string),
			},
			MeasureTypeList: []string{"SUM"},
		}
	}
	return items
}

func setMeasureFields(d *schema.ResourceData, fields []dapi.ReflectionMeasureField) error {
	items := make([]string, 0)
	for _, raw := range fields {
		for _, m := range raw.MeasureTypeList {
			if m == "SUM" {
				items = append(items, raw.Name)
			}
		}
	}
	return d.Set("measure_fields_sum", items)
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
