package dremio

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourcePhysicalDataset() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourcePhysicalDatasetCreate,
		ReadContext:   resourcePhysicalDatasetRead,
		UpdateContext: resourcePhysicalDatasetUpdate,
		DeleteContext: resourcePhysicalDatasetDelete,
		Schema: map[string]*schema.Schema{
			"path": {
				Type:     schema.TypeList,
				Required: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"type": {
				Type:     schema.TypeString,
				Required: true,
			},
			"field_delimiter": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"line_delimiter": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"quote": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"comment": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"escape": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"skip_first_line": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"extract_header": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"trim_header": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"auto_generate_column_names": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"sheet_name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"has_merged_cells": {
				Type:     schema.TypeBool,
				Optional: true,
			},
		},
	}
}

func resourcePhysicalDatasetCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	inputPath := d.Get("path").([]interface{})
	path := make([]string, len(inputPath))
	for i, elem := range inputPath {
		path[i] = elem.(string)
	}

	log.Printf("Fetching target by path: %v", path)
	original, err := c.GetCatalogEntityByPath(path)
	if err != nil {
		return diag.FromErr(err)
	}
	log.Printf("PDS target Id: %s, path: %v", original.Id, original.Path)
	pds, err := c.NewPhysicalDataset(original.Id, &dapi.NewPhysicalDatasetSpec{
		Path:   original.Path,
		Format: *getFormat(d),
	})
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(pds.Id)

	resourcePhysicalDatasetRead(ctx, d, m)

	return diags
}

func resourcePhysicalDatasetRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	pdsId := d.Id()

	pds, err := c.GetPhysicalDataset(pdsId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("path", pds.Path); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("type", pds.Type); err != nil {
		return diag.FromErr(err)
	}
	format := pds.Format

	if err := d.Set("field_delimiter", format.FieldDelimiter); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("line_delimiter", format.LineDelimiter); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("quote", format.Quote); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("comment", format.Comment); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("escape", format.Escape); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("skip_first_line", format.SkipFirstLine); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("extract_header", format.ExtractHeader); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("trim_header", format.TrimHeader); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("auto_generate_column_names", format.AutoGenerateColumnNames); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("sheet_name", format.SheetName); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("has_merged_cells", format.HasMergedCells); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourcePhysicalDatasetUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	sourceId := d.Id()

	_, err := c.UpdatePhysicalDataset(sourceId, &dapi.UpdatePhysicalDatasetSpec{
		Format: *getFormat(d),
	})
	if err != nil {
		return diag.FromErr(err)
	}
	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourcePhysicalDatasetRead(ctx, d, m)
}

func resourcePhysicalDatasetDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	pdsId := d.Id()

	err := c.DeleteCatalogItem(pdsId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}

func getFormat(d *schema.ResourceData) *dapi.PhysicalDatasetFormat {
	return &dapi.PhysicalDatasetFormat{
		Type:                    d.Get("type").(string),
		FieldDelimiter:          d.Get("field_delimiter").(string),
		LineDelimiter:           d.Get("line_delimiter").(string),
		Quote:                   d.Get("quote").(string),
		Comment:                 d.Get("comment").(string),
		Escape:                  d.Get("escape").(string),
		SkipFirstLine:           d.Get("skip_first_line").(bool),
		ExtractHeader:           d.Get("extract_header").(bool),
		TrimHeader:              d.Get("trim_header").(bool),
		AutoGenerateColumnNames: d.Get("auto_generate_column_names").(bool),
		SheetName:               d.Get("sheet_name").(string),
		HasMergedCells:          d.Get("has_merged_cells").(bool),
	}
}
