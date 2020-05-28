import mysql.connector
from datetime import datetime
import csv

db = mysql.connector.connect(
	host = "",
	user = "",
	passwd = "",
	database = ""
	)

mycursor = db.cursor()

# -------------------------------------- TORONTO ---------------------------------------------

def compare(csv, mSQL):
	print("""inside the compare function""")
	mSQL = list(mSQL)
	csv = list(csv)
	print(type(csv))
	print(type(mSQL))
	print("""the length of the CSV line is %d""" % len(csv))
	print("""the length of the SQL line is %d""" % len(mSQL))
	for idx, val in enumerate(csv):
		if val == 'NONE':
			csv[idx] = 0
		else:
			csv[idx] = val.replace("'", "")
	for index, value in enumerate(csv):
		print("""inside the enumerate function. enumerating CSV document""")
		try:
			print(f"""trying to convert the value {value} to integer""")
			csv[index] = int(value)
		except ValueError:
			print("""couldnt convert %s""" % value)
	try:
		for element in csv:
			mSQL.remove(element)
			print(f"""removed {element} from the 2nd list""")
	except ValueError:
		print(f"""{element} is not in the 2nd list""")
		return False 
	return True

# --- insert one entry and then modify it in order to automate column specification ---
def insert_into_mirror(list_from_csv):
	# --- creating three separate cursors in order to avoid 'unread results' error ---
	mycursor2 = db.cursor()
	mycursor3 = db.cursor()
	mycursor4 = db.cursor()
	# ^^^ created three separate cursors in order to avoid 'unread results' error ^^^

	# --- we can reference columns from a list and avoid having to manualy reference them in the query ---
	mirror_columns = ("_id", "AIR_CONDITIONING_TYPE", "AMENITIES_AVAILABLE", "ANNUAL_FIRE_ALARM_TEST_RECORDS", "ANNUAL_FIRE_PUMP_FLOW_TEST_RECORDS", 
		"APPROVED_FIRE_SAFETY_PLAN", "BALCONIES", "BARRIER_FREE_ACCESSIBILTY_ENTR", "BIKE_PARKING", "CONFIRMED_STOREYS", "CONFIRMED_UNITS", 
		"DATE_OF_LAST_INSPECTION_BY_TSSA", "DESCRIPTION_OF_CHILD_PLAY_AREA", "DESCRIPTION_OF_INDOOR_EXERCISE_ROOM", "DESCRIPTION_OF_OUTDOOR_REC_FACILITIES", 
		"ELEVATOR_PARTS_REPLACED", "ELEVATOR_STATUS", "EMERG_POWER_SUPPLY_TEST_RECORDS", "EXTERIOR_FIRE_ESCAPE", "FACILITIES_AVAILABLE", "FIRE_ALARM", "GARBAGE_CHUTES", 
		"GREEN_BIN_LOCATION", "HEATING_EQUIPMENT_STATUS", "HEATING_EQUIPMENT_YEAR_INSTALLED", "HEATING_TYPE", "INDOOR_GARBAGE_STORAGE_AREA", "INTERCOM", "IS_THERE_A_COOLING_ROOM", 
		"IS_THERE_EMERGENCY_POWER", "LAUNDRY_ROOM", "LAUNDRY_ROOM_HOURS_OF_OPERATION", "LAUNDRY_ROOM_LOCATION", "LOCKER_OR_STORAGE_ROOM", "NO_BARRIER_FREE_ACCESSBLE_UNITS", 
		"NO_OF_ACCESSIBLE_PARKING_SPACES", "NO_OF_ELEVATORS", "NO_OF_LAUNDRY_ROOM_MACHINES", "NON_SMOKING_BUILDING", "OUTDOOR_GARBAGE_STORAGE_AREA", "PARKING_TYPE", "PCODE", 
		"PET_RESTRICTIONS", "PETS_ALLOWED", "PROP_MANAGEMENT_COMPANY_NAME", "PROPERTY_TYPE", "RECYCLING_BINS_LOCATION", "RSN", "SEPARATE_GAS_METERS_EACH_UNIT", 
		"SEPARATE_HYDRO_METER_EACH_UNIT", "SEPARATE_WATER_METERS_EA_UNIT", "SITE_ADDRESS", "SPRINKLER_SYSTEM", "SPRINKLER_SYSTEM_TEST_RECORD", "SPRINKLER_SYSTEM_YEAR_INSTALLED", 
		"TSSA_TEST_RECORDS", "VISITOR_PARKING", "WARD", "WINDOW_TYPE", "YEAR_BUILT", "YEAR_OF_REPLACEMENT", "YEAR_REGISTERED", "NO_OF_STOREYS", "IS_THERE_EMERGENCY_POWER_q", 
		"NON_SMOKING_BUILDING_q", "NO_OF_UNITS", "NO_OF_ACCESSIBLEPARKING_SPACES", "FACILITIES_AVAILABLE_q", "IS_THERE_A_COOLING_ROOM_q", "NO_BARRIERFREE_ACCESSBLE_UNITS")
	# ^^^ just a list of columns ^^^

	# --- the entry below will create a new row with just one value in _id ---
	mycursor2.execute("""INSERT INTO mirror_csv_buildings (_id) VALUES (%s)""" % (list_from_csv[0]))
	db.commit()
	# ^^^ now we should have a new row and our primary key has increased by one ^^^

	# --- getting the primary key of the row we just created ---
	mycursor3.execute("SELECT MAX(primary_key) FROM mirror_csv_buildings")
	max_primary = list(mycursor3)
	primary_key_to_update = max_primary[0][0]
	# ^^^ primary_key_to_update is an integer from 1 to whatever is the maximum ^^^

	# --- the for loop below will update the most recent entry by populating every column, including the already-created _id (for the sake of simplicity)
	for index, value in enumerate(mirror_columns):
		print("""UPDATE mirror_csv_buildings SET %s = %s WHERE primary_key = %s""" % (value, list_from_csv[index], primary_key_to_update))
		if list_from_csv[index].strip() == 'NONE':
			print("""UPDATE mirror_csv_buildings SET %s = 0 WHERE primary_key = %s""" % (value, primary_key_to_update))
			mycursor4.execute("""UPDATE mirror_csv_buildings SET %s = 0 WHERE primary_key = %s""" % (value, primary_key_to_update))
			db.commit()
		else:
			mycursor4.execute("""UPDATE mirror_csv_buildings SET %s = '%s' WHERE primary_key = %s""" % (value, list_from_csv[index].replace("'", ""), primary_key_to_update))
			db.commit()
# ^^^ tested, works as intended ^^^

# --- the function below will:
# a) created "temporary" table that will consist of everything contained in mirror table, but without primary key or created, in order to simplify searching
# b) check if each line already exists in the "temporary" table by searching for the address
# c) if the search yields one or multiple results, it will check if each result matches the line. If at least one of them matches, it will not enter, if none of them match, it will enter
# d) at the end, it should drop the "temporary" table in order to not occupy memory
# ----
def add_new_file_toroto_db(file_path_string):

	# --- created "temporary" table that will act as a buffer for an easy search ---
	mycursor.execute("""CREATE TABLE temporary_to_build SELECT * FROM mirror_csv_buildings""")
	mycursor.execute("""ALTER TABLE temporary_to_build DROP COLUMN primary_key, DROP COLUMN created""")
	print("""created 'temporary' table that will act as a buffer""")
	#mycursor.execute("SELECT * FROM temporary_to_build") # <--- not sure this is necessary either
	#temp_results = list(mycursor) # <---- why was this necessary?
	# ^^^ created "temporary" table that will act as a buffer for an easy search ^^^

	# --- creating an iterable object (list) from the input CSV file ---
	file_obj = open(file_path_string)
	csv_reader = csv.reader(file_obj)
	rows = list(csv_reader)
	print("""created iterable object (list of lists) from the csv document located at %s""" % file_path_string)
	# ^^^ the object in the line above is a list of lists. can refer to items via rows[i][j] ^^^

	# --- creating variables that will work with  for loops below ---
	line_count = 0
	error_count = 0
	new_entries = 0
	# # ^^^ created variables that will work with  for loops below ^^^

	for line in rows:

		# --- querring whether there are entries with that address already line[51] refers to site address. The answer is a list ---
		print("""printing line""")
		print("""the length of this line is %d""" % (len(line)))
		print(line[51])
		print(f"""{line[51]}""")
		mycursor.execute("""SELECT COUNT(1) FROM temporary_to_build WHERE site_address = "%s" """ % (line[51].replace("'", "")))
		search_q = list(mycursor)
		# ^^^ querring whether there are entries with that address already line[51] refers to site address. The answer is a list ^^^

		# --- the first digit of the first entry represents how many items there are that match the search query above. There can be 0, >0, or error ---
		if search_q[0][0] == 0:
			# --- if there are 0 entries that match that query we should add the results ---
			insert_into_mirror(line)
			print("""inserted entry into the mirror table""")
			new_entries += 1
		elif search_q[0][0] > 0:
			# --- if there are in fact entries that match that query, we need to check if it is just 1 or multiple ---
			print("""entry(ies) exists""")

			# --- explicitly checking how many entries there are. First searching for everything that matches the address, then checking the length of that result ---
			print("""checking how many entries that match that address exist""")
			mycursor.execute("""SELECT * FROM temporary_to_build WHERE site_address = "%s" """ % (line[51].replace("'", "")))
			search_same_address = list(mycursor)
			how_many_items = len(search_same_address)
			print("""there are %d entries with that address""" % how_many_items)
			# ^^^ search_same_address is list of results ; how_many_items is an integer ^^^

			if how_many_items == 1:
				print("""I'm inside the if == 1 loop""")
				#if the only entry is exactly the same as the other
				line = [item.replace("'", "") for item in line]
				print("""the length of the line is %d""" % len(line))
				if compare(line, search_same_address[0]) == True:
					print("""entry already exists (if == 1 loop)""")
				else:
					#insert into table the new entry
					print("""inserting new entry (if == 1 loop)""")
					insert_into_mirror(line) # <------------------- haven't checked if this works as intended yet when I put it inside this function
					new_entries += 1
			else:
				# --- this is the event that there are multiple entries that match the query ---
				print("""I'm inside the else statement (multiple same addresses exist)""")
				i = 0
				line = [item.replace("'", "") for item in line]
				print("""the length of the line is %d""" % len(line))
				for index, value in enumerate(search_same_address):
					if compare(line, value) == True:
						print("""entry already exists will break out of the for loop""")
						i += 1
						break 
						# <----- haven't used break statement before, but it should work
					else: 
						print("""not exact match""")
						continue
				if i == 0:
					print("""none matches, so we need to add""")
					insert_into_mirror(line)
					new_entries += 1
				else:
					print("""passing because entry exists""")
		else:
			print("""some sort of error occurred""")
			error_count += 1
		print(f"""line {line_count} is ok""")
		line_count += 1
	print("""encountered %d error while checking""" % (error_count))
	print(type(line))
	#list 
	file_obj.close()
	mycursor.execute("""DROP TABLE temporary_to_build""")
	print("""in total, we added %d new entries""" % new_entries)
	# drop the "temporary" table here 

#file_p = "E:\\Code\\csv\\Apartment_Building_Registration_Data.csv"


#zz = add_new_file_toroto_db(file_p)

# --- the function below will inser ONE line into the actual_table ---
def insert_entry_into_actual(list_from_buffer):

	print("inside the function that inserts into the actual table")

	# --- creating some cursors --- 
	mycursor5 = db.cursor()
	mycursor6 = db.cursor()
	mycursor7 = db.cursor()
	# ^^^ just some additional cursors ^^^

	# --- useful lists --- 
	buffer_columns = ("_id", "AIR_CONDITIONING_TYPE", "AMENITIES_AVAILABLE", "BALCONIES", "BARRIER_FREE_ACCESSIBILTY_ENTR", "BIKE_PARKING", "EXTERIOR_FIRE_ESCAPE", 
		"FIRE_ALARM", "GARBAGE_CHUTES", "HEATING_TYPE", "INTERCOM", "LAUNDRY_ROOM", "LOCKER_OR_STORAGE_ROOM", "NO_OF_ELEVATORS", "PARKING_TYPE", "PETS_ALLOWED", 
		"PROP_MANAGEMENT_COMPANY_NAME", "PROPERTY_TYPE", "RSN", "SEPARATE_GAS_METERS_EACH_UNIT", "SEPARATE_HYDRO_METER_EACH_UNIT", "SEPARATE_WATER_METERS_EA_UNIT", 
		"SITE_ADDRESS", "SPRINKLER_SYSTEM", "VISITOR_PARKING", "WARD", "WINDOW_TYPE", "YEAR_BUILT", "YEAR_REGISTERED", "NO_OF_STOREYS", "IS_THERE_EMERGENCY_POWER_q", 
		"NON_SMOKING_BUILDING_q", "NO_OF_UNITS", "NO_OF_ACCESSIBLEPARKING_SPACES", "FACILITIES_AVAILABLE_q", "IS_THERE_A_COOLING_ROOM_q", "NO_BARRIERFREE_ACCESSBLE_UNITS", 
		"primary_key")

	amenities_av = ("Outdoor rec facilities", "Outdoor pool", "Indoor pool", "Indoor recreation room", "Sauna", "Indoor exercise room", "Child play area")
	
	parking_list = ("Underground Garage", "Ground Level Garage", "Garage accessible thru buildg", "Carport", "Surface Parking", "Parking Deck")

	parking_dictionary = {"Underground Garage": "underground_garage", "Ground Level Garage": "ground_level_garage", "Garage accessible thru buildg": "garage_thru_building", "Carport": "carport", 
		"Surface Parking": "sufrace_parking", "Parking Deck": "parking_deck"}

	actual_columns = ("_id", "AIR_CONDITIONING_TYPE", "AMENITIES_AVAILABLE", "BALCONIES", "BARRIER_FREE_ACCESSIBILTY_ENTR", "BIKE_PARKING", "EXTERIOR_FIRE_ESCAPE", "FIRE_ALARM", 
		"GARBAGE_CHUTES", "HEATING_TYPE", "INTERCOM", "LAUNDRY_ROOM", "LOCKER_OR_STORAGE_ROOM", "NO_OF_ELEVATORS", "PARKING_TYPE", "PETS_ALLOWED", "PROP_MANAGEMENT_COMPANY_NAME", 
		"PROPERTY_TYPE", "RSN", "SEPARATE_GAS_METERS_EACH_UNIT", "SEPARATE_HYDRO_METER_EACH_UNIT", "SEPARATE_WATER_METERS_EA_UNIT", "SITE_ADDRESS", "SPRINKLER_SYSTEM", "VISITOR_PARKING", 
		"WARD", "WINDOW_TYPE", "YEAR_BUILT", "YEAR_REGISTERED", "NO_OF_STOREYS", "IS_THERE_EMERGENCY_POWER_q", "NON_SMOKING_BUILDING_q", "NO_OF_UNITS", "NO_OF_ACCESSIBLEPARKING_SPACES", 
		"FACILITIES_AVAILABLE_q", "IS_THERE_A_COOLING_ROOM_q", "NO_BARRIERFREE_ACCESSBLE_UNITS", "build_primary_key", "outdoor_rec_fac", "outdoor_pool", "indoor_pool", "indoor_rec_room", 
		"sauna", "indoor_exercise_room", "child_play_area", "underground_garage", "ground_level_garage", "garage_thru_building", "carport", "sufrace_parking", "parking_deck", "indoor_bike_parking", 
		"outdoor_bike_parking", "entry_type", "from_table", "actual_table_primary_key")

	dictionary_amenities = {"Outdoor rec facilities": "outdoor_rec_fac", "Outdoor pool": "outdoor_pool", "Indoor pool": "indoor_pool", "Indoor recreation room": "indoor_rec_room", "Sauna": "sauna", 
		"Indoor exercise room": "indoor_exercise_room", "Child play area": "child_play_area"}
	# ^^^ useful lists ^^^

	# --- inserting one value into one row that will later be modified ---
	mycursor5.execute("""INSERT INTO actual_table (_id) VALUES (%s)""" % (list_from_buffer[0]))
	db.commit()
	print("inserted into _id value %s" % (list_from_buffer[0]))
	# ^^^ now we should have a new row and our primary key has increased by one ^^^

	# --- getting the primary key of the row we just created ---
	mycursor6.execute("SELECT MAX(actual_table_primary_key) FROM actual_table")
	max_primary = list(mycursor6)
	primary_key_to_update = max_primary[0][0]
	print("the max primary key is %d" % (primary_key_to_update))
	# ^^^ primary_key_to_update is an integer from 1 to whatever is the maximum ^^^

	# --- counting errors ---
	error_count = 0

	# --- the for loop below will iterate over all the columns in the buffer and do specific things for specified columns and general task for all other ---
	print("entering the for loop")
	for index, value in enumerate(buffer_columns):
		# --- value is the column name ---

		# --- here we are handling AMENITIES_AVAILABLE column since we have to potentially split it into 7 other columns ---
		if value == "AMENITIES_AVAILABLE":
			print("inside the amenities condition")

			# --- if the value is blank, it will insert 0 there ---
			if list_from_buffer[index] == "":
				mycursor7.execute("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (value, primary_key_to_update))
				db.commit()

			# --- the condition below is for the header. haven't check if this works---
			elif list_from_buffer[index] == value:
				print("dealing with the header")
				mycursor7.execute("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))
				db.commit()
				print("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))

			# --- otherwise, (if the value is not blank and not a header), it will test whether each element of the amenities_av list is present in the string, 
			# if yes, it will get column name from the dictionary dictionary_amenities and insert 1 there ---
			else:
				for item in amenities_av:
				# --- each item in this for loop is a unique amenity ---
					print("inside the for loop that iterates over the amenities")
					try:
						# --- the purpose of the list_from_buffer[index].index(item) is so that if is possible to do that, it will know which item worked and insert it into the appropriate column from the dictionary_amenities ---
						print("found the item %s with index %s" % (item, list_from_buffer[index].index(item)))
						mycursor7.execute("""UPDATE actual_table SET %s = 1 WHERE actual_table_primary_key = %s""" % (dictionary_amenities.get(item), primary_key_to_update))
						db.commit()
						print("""UPDATE actual_table SET %s = 1 WHERE actual_table_primary_key = %s""" % (dictionary_amenities.get(item), primary_key_to_update))
					except ValueError:
						# --- if it cannot find the amenity, it will give error, so we will have to insert 0 into that column ---
						print("couldn't find %s" % (item))
						mycursor7.execute("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (dictionary_amenities.get(item), primary_key_to_update))
						db.commit()
						print("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (dictionary_amenities.get(item), primary_key_to_update))

		# --- here we are handling BIKE_PARKING column since we have to split it into two ---
		elif value == "BIKE_PARKING":
			
			# --- same idea as in the amenities condition. first we check if it is empty or if it says "Not Available", if yes then we insert 0 into that column
			if list_from_buffer[index] == "" or list_from_buffer[index] == "Not Available":
				print("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (value, primary_key_to_update))
				mycursor7.execute("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (value, primary_key_to_update))
				db.commit()

			# --- the condition below is for the header. haven't check if this works---
			elif list_from_buffer[index] == value:
				print("dealing with the header")
				mycursor7.execute("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))
				db.commit()
				print("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))

			else:
				try:
					pt = list_from_buffer[index].split("and")
					num_indr_p = int(pt[0][0:pt[0].index("indoor")])
					num_outdr_p = int(pt[1][0:pt[1].index("outdoor")])
					print("indoor spots == %d" % (num_indr_p))
					print("outdoor spots == %d" % (num_outdr_p))
					# "indoor_bike_parking", "outdoor_bike_parking"
					mycursor7.execute("""UPDATE actual_table SET indoor_bike_parking = '%d', outdoor_bike_parking = '%d' WHERE actual_table_primary_key = %s""" % (num_indr_p, num_outdr_p, primary_key_to_update))
					db.commit()
					print("""UPDATE actual_table SET indoor_bike_parking = '%d', outdoor_bike_parking = '%d' WHERE actual_table_primary_key = %s""" % (num_indr_p, num_outdr_p, primary_key_to_update))
				except ValueError:
					pt = list_from_buffer[index].split("and")
					num_indr_p = pt[0][0:pt[0].index("indoor")]
					num_outdr_p = pt[1][0:pt[1].index("outdoor")]
					num_indr_p = num_indr_p.lstrip(" ")
					num_outdr_p = num_outdr_p.lstrip(" ")
					if num_indr_p == "":
						print("""UPDATE actual_table SET indoor_bike_parking = 0 WHERE actual_table_primary_key = %s""" % (primary_key_to_update))
						mycursor7.execute("""UPDATE actual_table SET indoor_bike_parking = 0 WHERE actual_table_primary_key = %s""" % (primary_key_to_update))
						db.commit()
					if num_outdr_p == "":
						print("""UPDATE actual_table SET outdoor_bike_parking = 0 WHERE actual_table_primary_key = %s""" % (primary_key_to_update))
						mycursor7.execute("""UPDATE actual_table SET outdoor_bike_parking = 0 WHERE actual_table_primary_key = %s""" % (primary_key_to_update))
						db.commit()
					else:
						print("some sort of error occurred")
						print("num of indoor parking spots is %s" % (num_indr_p))
						print("num of outdoor parking spots is %s" % (num_outdr_p))
						print(type(num_indr_p))
						print(type(num_outdr_p))
						error_count += 1

		# --- here we are handling PARKING_TYPE column since we ahve to potentially split it into 6 columns ---
		elif value == "PARKING_TYPE":

			# --- same idea as in the amenities condition. first we check if it is empty, if yes then we insert 0 into that column
			if list_from_buffer[index] == "":
				print("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (value, primary_key_to_update))
				mycursor7.execute("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (value, primary_key_to_update))
				db.commit()

			# --- the condition below is for the header. haven't check if this works---
			elif list_from_buffer[index] == value:
				print("dealing with the header")
				mycursor7.execute("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))
				db.commit()
				print("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))

			# --- otherwise, (if the value is not blank and not a header), it will check if it can find any of the parking types in the string,
			# if yes, it will look up the column name from the parking_dictionary and insert 1 into there
			else:
				for item in parking_list:
				# --- each item is a parking type 
					try:
						# --- the purpose of the list_from_buffer[index].index(item) is so that if is possible to do that, it will know which item worked and insert it into the appropriate column from the dictionary_amenities ---
						print("found the item %s with index %s" % (item, list_from_buffer[index].index(item)))
						mycursor7.execute("""UPDATE actual_table SET %s = 1 WHERE actual_table_primary_key = %s""" % (parking_dictionary.get(item), primary_key_to_update))
						db.commit()
						print("""UPDATE actual_table SET %s = 1 WHERE actual_table_primary_key = %s""" % (parking_dictionary.get(item), primary_key_to_update))
					except (ValueError, AttributeError):
						# --- if it cannot find that particular parking type (item), it will give error, so we will have to insert 0 into that column ---
						print("couldn't find %s" % (item))
						mycursor7.execute("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (parking_dictionary.get(item), primary_key_to_update))
						db.commit()
						print("""UPDATE actual_table SET %s = 0 WHERE actual_table_primary_key = %s""" % (parking_dictionary.get(item), primary_key_to_update))

		# --- here we are handling primary_key. in the actual table, the primary key of the csv_mirror table is not actually considered a primary key
		# that is because there can only be one primary key on each column, but it is useful to have it anyways
		# all we do is simply insert primary_key into actual_table_primary_key ---
		elif value == "primary_key":
			print("""UPDATE actual_table SET build_primary_key = %s WHERE actual_table_primary_key = %s""" % (list_from_buffer[index], primary_key_to_update))
			mycursor7.execute("""UPDATE actual_table SET build_primary_key = %s WHERE actual_table_primary_key = %s""" % (list_from_buffer[index], primary_key_to_update))
			db.commit()

		# --- for all other columns we have this else condition ---
		else:
			mycursor7.execute("""UPDATE actual_table SET %s = '%s' WHERE actual_table_primary_key = %s""" % (value, list_from_buffer[index], primary_key_to_update))
			db.commit()	

	# --- finishing touches, adding meta-information ---
	mycursor7.execute("""UPDATE actual_table SET entry_type = 'building', from_table = 'mirror_csv_buildings' WHERE actual_table_primary_key = %s""" % (primary_key_to_update))
	db.commit()


	print("total amount of errors = %s" % (error_count))

		
# need a function that will go line by line and insert each entry into the database that will be used for analytics
# it should check if that entry already exists. Now that can be done simply by checking the primary key 
# probably a good idea to first create a 'buffer' table like in the big function above

def synchronize_buildings_actual_table(table):
	# actually, this does not realy need an input variable

	# --- creating additional cursors ---
	mycursor2 = db.cursor()
	mycursor3 = db.cursor()
	mycursor4 = db.cursor()
	mycursor8 = db.cursor()

	# --- finding the min(primary_key) and max(primary_key) of the mirror table ---
	mycursor.execute("""SELECT MIN(primary_key) FROM mirror_csv_buildings""")
	min_primary = list(mycursor)
	min_primary_key = int(min_primary[0][0])
	mycursor2.execute("""SELECT MAX(primary_key) FROM mirror_csv_buildings""")
	max_primary = list(mycursor2)
	max_primary_key = int(max_primary[0][0])
	print("""the minimum is %d""" % (min_primary_key))
	print("""the maximum is %d""" % (max_primary_key))
	print(type(min_primary_key)) 
	print(type(max_primary_key))
	# ^^^ also checking if the min and max are int. works as inteded ^^^

	# --- creating 'buffer' table without empty columns; splitting into different lines so as to make it easier to read ---
	print("""creating 'buffer' table""")
	mycursor3.execute("""CREATE TABLE buffer_buildings SELECT * FROM mirror_csv_buildings""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN ANNUAL_FIRE_ALARM_TEST_RECORDS, DROP COLUMN ANNUAL_FIRE_PUMP_FLOW_TEST_RECORDS, DROP APPROVED_FIRE_SAFETY_PLAN""") 
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN CONFIRMED_STOREYS, DROP COLUMN CONFIRMED_UNITS, DROP DATE_OF_LAST_INSPECTION_BY_TSSA""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN DESCRIPTION_OF_CHILD_PLAY_AREA, DROP COLUMN DESCRIPTION_OF_INDOOR_EXERCISE_ROOM, DROP DESCRIPTION_OF_OUTDOOR_REC_FACILITIES""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN ELEVATOR_PARTS_REPLACED, DROP COLUMN ELEVATOR_STATUS, DROP EMERG_POWER_SUPPLY_TEST_RECORDS""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN FACILITIES_AVAILABLE, DROP COLUMN GREEN_BIN_LOCATION, DROP HEATING_EQUIPMENT_STATUS""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN HEATING_EQUIPMENT_YEAR_INSTALLED, DROP INDOOR_GARBAGE_STORAGE_AREA""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN IS_THERE_A_COOLING_ROOM, DROP COLUMN IS_THERE_EMERGENCY_POWER, DROP LAUNDRY_ROOM_HOURS_OF_OPERATION""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN LAUNDRY_ROOM_LOCATION, DROP COLUMN NO_BARRIER_FREE_ACCESSBLE_UNITS, DROP NO_OF_ACCESSIBLE_PARKING_SPACES""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN NO_OF_LAUNDRY_ROOM_MACHINES, DROP COLUMN NON_SMOKING_BUILDING, DROP OUTDOOR_GARBAGE_STORAGE_AREA""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN PCODE, DROP COLUMN PET_RESTRICTIONS, DROP RECYCLING_BINS_LOCATION""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN SPRINKLER_SYSTEM_TEST_RECORD, DROP COLUMN SPRINKLER_SYSTEM_YEAR_INSTALLED, DROP TSSA_TEST_RECORDS""")
	mycursor3.execute("""ALTER TABLE buffer_buildings DROP COLUMN YEAR_OF_REPLACEMENT, DROP COLUMN created""")
	print("""created 'buffer' table""")
	# ^^^ This is actually somewhat computationally intensive it seems. Should drop the table in the end ^^^
	
	# --- these are the columns from the "buffer" table --- 
	buffer_columns = ("_id", "AIR_CONDITIONING_TYPE", "AMENITIES_AVAILABLE", "BALCONIES", "BARRIER_FREE_ACCESSIBILTY_ENTR", "BIKE_PARKING", "EXTERIOR_FIRE_ESCAPE", 
		"FIRE_ALARM", "GARBAGE_CHUTES", "HEATING_TYPE", "INTERCOM", "LAUNDRY_ROOM", "LOCKER_OR_STORAGE_ROOM", "NO_OF_ELEVATORS", "PARKING_TYPE", "PETS_ALLOWED", 
		"PROP_MANAGEMENT_COMPANY_NAME", "PROPERTY_TYPE", "RSN", "SEPARATE_GAS_METERS_EACH_UNIT", "SEPARATE_HYDRO_METER_EACH_UNIT", "SEPARATE_WATER_METERS_EA_UNIT", 
		"SITE_ADDRESS", "SPRINKLER_SYSTEM", "VISITOR_PARKING", "WARD", "WINDOW_TYPE", "YEAR_BUILT", "YEAR_REGISTERED", "NO_OF_STOREYS", "IS_THERE_EMERGENCY_POWER_q", 
		"NON_SMOKING_BUILDING_q", "NO_OF_UNITS", "NO_OF_ACCESSIBLEPARKING_SPACES", "FACILITIES_AVAILABLE_q", "IS_THERE_A_COOLING_ROOM_q", "NO_BARRIERFREE_ACCESSBLE_UNITS", 
		"primary_key")
	# ^^^ len(buffer_columns) = 39 ^^^

	# --- should have a for loop that iterates over the range(min_primary_key, max_primary_key) below with if statements based on the column +1 is there in order to correct the range --- 
	for element in range(min_primary_key, max_primary_key + 1):

		# --- checking if there are already entries with that primary number ---
		mycursor4.execute("""SELECT COUNT(1) FROM actual_table WHERE build_primary_key = "%s" """ % (element)) 
		search_q = list(mycursor)
		search_hits = search_q[0][0]
		# ^^^ search_hits is an integer and gives a number of rows that match that primary_key ^^^

		if search_hits == 0:
			mycursor8.execute("""SELECT * FROM buffer_buildings WHERE primary_key = '%s' """ % (element))
			search_rr = list(mycursor8)
			search_one_line = search_rr[0]
			insert_entry_into_actual(search_one_line)
		else: 
			print("""entry with that primary key already exists""")

	mycursor.execute("""DROP TABLE buffer_buildings""")


