package dremio

func interfaceListToStringList(itemsRaw []interface{}) []string {
	items := make([]string, len(itemsRaw))
	for i, raw := range itemsRaw {
		items[i] = raw.(string)
	}
	return items
}
