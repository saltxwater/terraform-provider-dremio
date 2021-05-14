package dremio

import (
	"context"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceEntityWiki() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceEntityWikiCreate,
		ReadContext:   resourceEntityWikiRead,
		UpdateContext: resourceEntityWikiUpdate,
		DeleteContext: resourceEntityWikiDelete,
		Schema: map[string]*schema.Schema{
			"entity_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"text": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}

func resourceEntityWikiCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	entityId := d.Get("entity_id").(string)
	text := d.Get("text").(string)
	wikiVersion := 0
	wikiBody, err := c.GetEntityWiki(entityId)
	if err != nil {
		// Assume failed because no tags already exist?
	} else {
		wikiVersion = wikiBody.Version
	}

	err = c.SetEntityWiki(entityId, text, wikiVersion)

	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(entityId)

	return resourceEntityWikiRead(ctx, d, m)
}

func resourceEntityWikiRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	entityId := d.Id()

	text, err := c.GetEntityWiki(entityId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("text", text)

	return diags
}

func resourceEntityWikiUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	entityId := d.Id()
	text := d.Get("text").(string)
	wikiBody, err := c.GetEntityWiki(entityId)
	if err != nil {
		return diag.FromErr(err)
	}

	err = c.SetEntityWiki(entityId, text, wikiBody.Version)

	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourceEntityWikiRead(ctx, d, m)
}

func resourceEntityWikiDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	entityId := d.Id()
	wikiBody, err := c.GetEntityWiki(entityId)
	if err == nil {
		err := c.SetEntityWiki(entityId, "", wikiBody.Version)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	d.SetId("")

	return diags
}
