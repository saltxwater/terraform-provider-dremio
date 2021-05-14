---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "dremio_virtual_dataset Resource - terraform-provider-dremio"
subcategory: ""
description: |-
  
---

# dremio_virtual_dataset (Resource)





<!-- schema generated by tfplugindocs -->
## Schema

### Required

- **name** (String)
- **parent_id** (String)
- **sql** (String)

### Optional

- **id** (String) The ID of this resource.
- **sql_context** (List of String)

### Read-Only

- **fields** (List of Object, Sensitive) (see [below for nested schema](#nestedatt--fields))
- **path** (List of String)
- **query_path** (String)

<a id="nestedatt--fields"></a>
### Nested Schema for `fields`

Read-Only:

- **name** (String)
- **type** (String)

