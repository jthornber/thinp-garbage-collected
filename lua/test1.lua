local pool = create_pool("./pool_dir", 1024, 1024 * 1024)

local thin_id = pool:create_thin(100)
print("Created thin volume with ID:", thin_id)

local thick_id = pool:create_thick(200)
print("Created thick volume with ID:", thick_id)

local snap_id = pool:create_snap(thin_id)
print("Created snapshot of thin volume with ID:", snap_id)

local write_mappings = pool:get_write_mapping(thin_id, 0, 50)
for _, mapping in ipairs(write_mappings) do
	print(
		"Write mapping - VBlock:",
		mapping.vblock,
		"PBlock Begin:",
		mapping.mapping:b(),
		"PBlock End:",
		mapping.mapping:e(),
		"Snap Time:",
		mapping.mapping:snap_time()
	)
end

pool:discard(thin_id, 0, 25)
print("Discarded mappings from thin volume")

pool:delete_thin(thin_id)
print("Deleted thin volume with ID:", thin_id)
