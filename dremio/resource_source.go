package dremio

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func makeResourceSource(sourceType string, configFn configCreator, configReaderFn configReader, s map[string]*schema.Schema) *schema.Resource {
	return &schema.Resource{
		CreateContext: makeResourceSourceCreate(sourceType, configFn, configReaderFn),
		ReadContext:   makeResourceSourceRead(configReaderFn),
		UpdateContext: makeResourceSourceUpdate(configFn, configReaderFn),
		DeleteContext: resourceSourceDelete,
		Schema:        s,
	}
}

type configCreator func(d *schema.ResourceData) interface{}
type configReader func(source *dapi.Source, d *schema.ResourceData) diag.Diagnostics

func makeResourceSourceCreate(sourceType string, configFn configCreator, configReaderFn configReader) schema.CreateContextFunc {
	readFn := makeResourceSourceRead(configReaderFn)
	return func(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
		c := m.(*dapi.Client)

		// Warning or errors can be collected in a slice type
		var diags diag.Diagnostics

		space, err := c.NewSource(&dapi.NewSourceSpec{
			Name:        d.Get("name").(string),
			Description: d.Get("description").(string),
			Type:        sourceType,
			Config:      configFn(d),
		})
		if err != nil {
			return diag.FromErr(err)
		}

		d.SetId(space.Id)

		readFn(ctx, d, m)

		return diags
	}
}

func makeResourceSourceRead(configReaderFn configReader) schema.ReadContextFunc {
	return func(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
		c := m.(*dapi.Client)

		sourceId := d.Id()

		source, err := c.GetSource(sourceId)
		if err != nil {
			return diag.FromErr(err)
		}
		log.Printf("MakeResourceSourceRead: %#v", source)

		if err := d.Set("name", source.Name); err != nil {
			return diag.FromErr(err)
		}

		if err := d.Set("description", source.Description); err != nil {
			return diag.FromErr(err)
		}

		return configReaderFn(source, d)
	}
}

func makeResourceSourceUpdate(configFn configCreator, configReaderFn configReader) schema.UpdateContextFunc {
	readFn := makeResourceSourceRead(configReaderFn)
	return func(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
		c := m.(*dapi.Client)

		sourceId := d.Id()

		_, err := c.UpdateSource(sourceId, &dapi.UpdateSourceSpec{
			Description: d.Get("description").(string),
			Config:      configFn(d),
		})
		if err != nil {
			return diag.FromErr(err)
		}
		d.Set("last_updated", time.Now().Format(time.RFC850))

		return readFn(ctx, d, m)
	}
}

func resourceSourceDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	sourceId := d.Id()

	err := c.DeleteCatalogItem(sourceId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return diags
}
