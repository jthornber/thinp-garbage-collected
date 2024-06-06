-- Simple Lua REPL
-- Function to read a line of input from the user
local function read_input(prompt)
	io.write(prompt)
	io.flush()
	return io.read()
end

local function banner()
	print("Welcome to thinp-userland development repl.")
end

-- Function to evaluate Lua code and print the result or error
local function eval_and_print(input)
	-- First we try the input as an expression
	local chunk, err = load("return " .. input, "stdin")
	if not chunk then
		-- otherwise we try it as a statement
		chunk, err = load(input, "stdin")
	end

	if chunk then
		local success, result = pcall(chunk)
		if success then
			if result ~= nil then
				print(result)
			end
		else
			print("Error: " .. result)
		end
	else
		print("Error: " .. err)
	end
end

banner()

-- Main REPL loop
while true do
	local input = read_input("> ")

	if input == nil then
		-- Handle Ctrl-D (EOF)
		print("Exiting REPL")
		break
	end

	eval_and_print(input)
end
