package dremio

import (
	"context"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	dapi "github.com/saltxwater/go-dremio-api-client"
)

func resourceEntityTags() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceEntityTagsCreate,
		ReadContext:   resourceEntityTagsRead,
		UpdateContext: resourceEntityTagsUpdate,
		DeleteContext: resourceEntityTagsDelete,
		Schema: map[string]*schema.Schema{
			"entity_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"tags": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}

func getTags(d *schema.ResourceData) []string {
	rawTags := d.Get("tags").([]interface{})
	tags := make([]string, len(rawTags))
	for i, name := range rawTags {
		tags[i] = name.(string)
	}
	return tags
}

func resourceEntityTagsCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	entityId := d.Get("entity_id").(string)
	tags := getTags(d)
	tagVersion := ""
	tagBody, err := c.GetEntityTags(entityId)
	if err != nil {
		// Assume failed because no tags already exist?
	} else {
		tagVersion = tagBody.Version
	}

	err = c.SetEntityTags(entityId, tags, tagVersion)

	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(entityId)

	return resourceEntityTagsRead(ctx, d, m)
}

func resourceEntityTagsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	entityId := d.Id()

	tags, err := c.GetEntityTags(entityId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("tags", tags.Tags)

	return diags
}

func resourceEntityTagsUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	entityId := d.Id()
	tags := getTags(d)
	tagBody, err := c.GetEntityTags(entityId)
	if err != nil {
		return diag.FromErr(err)
	}

	err = c.SetEntityTags(entityId, tags, tagBody.Version)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("last_updated", time.Now().Format(time.RFC850))

	return resourceEntityTagsRead(ctx, d, m)
}

func resourceEntityTagsDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dapi.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	entityId := d.Id()
	tagBody, err := c.GetEntityTags(entityId)
	if err == nil {
		err = c.SetEntityTags(entityId, []string{}, tagBody.Version)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	d.SetId("")

	return diags
}
