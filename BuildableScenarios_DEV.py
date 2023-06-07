from arcgis.mapping import MapServiceLayer, MapFeatureLayer
import arcpy
from arcgis import GIS
from arcgis.geometry import filters
import time
import os, sys
import pandas as pd
from datetime import date
import datetime

today = date.today()

# Change test

# YY/mm/dd
datestr = today.strftime("%Y%m%d")

inAOI = arcpy.GetParameter(0)
inAOI_buff_dist = arcpy.GetParameterAsText(1)
SR = arcpy.GetParameterAsText(2)
projectName_ID = arcpy.GetParameterAsText(3)
new_buildableID = arcpy.GetParameterAsText(4)
previous_project = arcpy.GetParameterAsText(7)
value_table = arcpy.GetParameter(9)

new_buildableID = str(new_buildableID)

# Debugging values
# inAOI = r"G:\Projects\USA_North\Northern_Bobwhite_Geenex\05_GIS\053_Data\Boundary_ALTA_Northern_Bobwhite_20220407.shp"
# inAOI = arcpy.MakeFeatureLayer_management(inAOI, "inAOI_layer")
# SR = arcpy.SpatialReference(6475)
# projectWithNoID = False
# newProjectNameWithNoID = ""
# projectName_ID = "Northern Bobwhite - 3207"
# new_buildableID = "24"
# previousProjectName_ID = "Northern Bobwhite - 3207"
# value_table = arcpy.ValueTable(6)
#
# aprx = arcpy.mp.ArcGISProject(r"G:\Users\Ardy\GIS\APRX\PersonalAPRX.aprx")
#
# path = r"G:\Projects\USA_North\Northern_Bobwhite_Geenex\05_GIS\053_Data\Karst_Points_Survey_Mod_to_High_20210129.shp"
#
# value_table.addRow(f"'Test Points' '{path}' '35 Feet' 'FULL' 'Testing writing to Points' 'Environmental'")
# End of debugging values

projectName = projectName_ID.split(' - ')[0]

if " " in projectName:
    project_gdb_name = projectName.replace(" ", "_")
elif "_" in projectName:
    project_gdb_name = projectName.replace("-", "_")
else:
    project_gdb_name = projectName

newProjectID = projectName_ID.split(' - ')[1]

# Create User Working Directory
aprx = arcpy.mp.ArcGISProject('CURRENT')
aprx_folder = aprx.homeFolder

# If else to create home folder for tool's run
if "05_GIS" not in aprx_folder:
    arcpy.AddMessage(f"Found home folder {aprx_folder}")
    project_data_folder = os.path.join(aprx_folder, r"05_GIS\053_Data\working")
    outputGDB = arcpy.CreateFileGDB_management(project_data_folder,
                                               "BL_v" + new_buildableID + "_" + project_gdb_name + "_" + datestr)
else:
    project_data_folder = os.path.join(aprx_folder, r"053_Data\working")
    outputGDB = arcpy.CreateFileGDB_management(project_data_folder,
                                               "BL_v" + new_buildableID + "_" + project_gdb_name + "_" + datestr)

arcpy.AddMessage("Created GDB for Project in: {}".format(outputGDB))

startTime = time.time()
lyrNamesDict = {}
setbackList = []
bufferresultList = []
tmpbufferList = []
selectedFeatures_Count = []
sde_table_path = r"G:\General\GIS\SDEConnections\DEV_and_STG\SDHQRAGDBSTG01_webmap.sde\DBO.Constraint_Tracker_Solar"
constraint_tracker_points = r"G:\General\GIS\SDEConnections\DEV_and_STG\SDHQRAGDBSTG01_webmap.sde\DBO.Constraint_Tracker_Solar_point_WGS84"
constraint_tracker_lines = r"G:\General\GIS\SDEConnections\DEV_and_STG\SDHQRAGDBSTG01_webmap.sde\DBO.Constraint_Tracker_Solar_line_WGS84"
constraint_tracker_polys = r"G:\General\GIS\SDEConnections\DEV_and_STG\SDHQRAGDBSTG01_webmap.sde\DBO.Constraint_Tracker_Solar_polygon_WGS84"

arcpy.env.overwriteOutput = True
arcpy.env.workspace = sde_table_path

arcpy.env.outputMFlag = "Disabled"
arcpy.env.outputZFlag = "Disabled"

arcpy.env.outputCoordinateSystem = SR
AOI_SR = arcpy.env.outputCoordinateSystem

# creating feature datasets within the working GDB
arcpy.CreateFeatureDataset_management(outputGDB, "Features", AOI_SR)
arcpy.CreateFeatureDataset_management(outputGDB, "Buffers", AOI_SR)
arcpy.CreateFeatureDataset_management(outputGDB, "Buildable_Land", AOI_SR)

outputGDB = str(outputGDB)

# creating buffer for AOI and get the extent of the AOI
t0 = time.time()
outaoiPath = outputGDB + '/' + 'Buffers' + '/inAOI_' + f'{str(inAOI_buff_dist).replace(" ", "_").replace(".", "_")}'
arcpy.Buffer_analysis(inAOI, outaoiPath, inAOI_buff_dist, "FULL", "ROUND", "ALL", "", "PLANAR")
arcpy.RepairGeometry_management(outaoiPath)
arcpy.AddMessage(f'Created AOI Buffer of {str(inAOI_buff_dist).replace(" ", "_")}')
inAOI_Extent = arcpy.Describe(inAOI).extent
inAOI_buff_Extent = arcpy.Describe(outaoiPath).extent

sa_filter = filters.intersects(geometry=inAOI_buff_Extent.polygon, sr=inAOI_buff_Extent.spatialReference)
t1 = time.time()
arcpy.AddMessage(f"...   ... done in {datetime.timedelta(seconds=t1 - t0)}")

# log into GIS using the user's Pro session account
gis = GIS("pro")

setbackGeomsAndFeatureCountDict = {}

search_where = f"projectname = '{projectName}'" + "AND project_id = {} AND Buildable_Version = {}".format(newProjectID,
                                                                                                          new_buildableID)

sde_project_name = None
sde_project_id = None
sde_buildable_id = None

t0 = time.time()
arcpy.AddMessage("Looking for duplicate rows in SDE")
with arcpy.da.SearchCursor(sde_table_path, ["projectname", "project_id", "Buildable_Version"],
                           where_clause=search_where) as cursor:
    # Fix for loop issue - going through duplicates twice, handle case if no duplicates and pass to next step
    for row in cursor:
        sde_project_name = row[0]
        sde_project_id = row[1]
        sde_buildable_id = row[2]

if sde_project_name is None and sde_project_id is None and sde_buildable_id is None:
    arcpy.AddMessage("No duplicates found")
    sde_project_name = None
    sde_project_id = None
    sde_buildable_id = None

buildable_id_len = len(str(sde_buildable_id))

if buildable_id_len == 1:
    sde_buildable_id = '0' + str(sde_buildable_id)

# if project already exists, make sure constraints are not duplicate within the sde constraint tables
if projectName == sde_project_name and newProjectID == str(sde_project_id) and new_buildableID == str(sde_buildable_id):
    arcpy.AddMessage("Found duplicate constraints and removing them.")
    for i in range(0, value_table.rowCount):
        constraintName = value_table.getValue(i, 0).strip()
        constraintName = constraintName.replace(' ', '_')
        tmpPath = value_table.getValue(i, 1).strip()
        tmpPathDesc = arcpy.Describe(tmpPath)
        fullPath = tmpPathDesc.catalogPath
        constraintPath = fullPath.replace("\\", "/")
        Dist = value_table.getValue(i, 2).strip()
        tmpDist = Dist.split()
        buffDist = tmpDist[0]
        buffMetric = tmpDist[1]
        buffType = value_table.getValue(i, 3).strip()
        notes = value_table.getValue(i, 4).strip()
        symbology = value_table.getValue(i, 5).strip()
        countOfFeatures = 0
        constraint_shape_type = tmpPathDesc.shapeType

        # delete duplicate feature type records before writing them
        updateWhere_constraint = f"projectname = '{projectName}'" + "AND project_id = {} AND Buildable_Version = {}".format(
            newProjectID, new_buildableID)

        # get parent global id for feature from constraint tracker solar
        with arcpy.da.SearchCursor(sde_table_path, "GlobalID", where_clause=updateWhere_constraint) as cursor:
            for row in cursor:
                feature_global_id = row[0]

                deleteWhere_constraint = f"Symbology = '{symbology}' AND ParentGlobalID = '{feature_global_id}'"

                if constraint_shape_type == 'Point':
                    with arcpy.da.UpdateCursor(constraint_tracker_points, ["Symbology", "ParentGlobalID"],
                                               where_clause=deleteWhere_constraint) as uCur:
                        for row in uCur:
                            uCur.deleteRow()

                if constraint_shape_type == 'Polyline':
                    with arcpy.da.UpdateCursor(constraint_tracker_lines, ["Symbology", "ParentGlobalID"],
                                               where_clause=deleteWhere_constraint) as uCur:
                        for row in uCur:
                            uCur.deleteRow()

                if constraint_shape_type == 'Polygon':
                    with arcpy.da.UpdateCursor(constraint_tracker_polys, ["Symbology", "ParentGlobalID"],
                                               where_clause=deleteWhere_constraint) as uCur:
                        for row in uCur:
                            uCur.deleteRow()

    arcpy.AddMessage("Finished removing duplicate constraints.")

# Execute MakeTableView and delete existing records & update
deleteWhere = f"projectname = '{projectName}'" + "AND project_id = {} AND Buildable_Version = {}".format(newProjectID,
                                                                                                         new_buildableID)

with arcpy.da.UpdateCursor(sde_table_path, ["projectname", "project_id", "Buildable_Version", "GlobalID"],
                           where_clause=deleteWhere) as cursor:
    for row in cursor:
        arcpy.AddMessage(f"Found & deleting duplicate row -- {row}")
        cursor.deleteRow()

t1 = time.time()
arcpy.AddMessage(f"...   ... done in {datetime.timedelta(seconds=t1 - t0)}")

t0 = time.time()
arcpy.AddMessage("Inputting constraints into SDE Table")

# Creating table in SDE so that constraints are saved at the beginning of process
for i in range(0, value_table.rowCount):
    constraintName = value_table.getValue(i, 0).strip()
    constraintName = constraintName.replace(' ', '_')
    tmpPath = value_table.getValue(i, 1).strip()
    tmpPathDesc = arcpy.Describe(tmpPath)
    fullPath = tmpPathDesc.catalogPath
    constraintPath = fullPath.replace("\\", "/")
    Dist = value_table.getValue(i, 2).strip()
    tmpDist = Dist.split()
    buffDist = tmpDist[0]
    buffMetric = tmpDist[1]
    buffType = value_table.getValue(i, 3).strip()
    notes = value_table.getValue(i, 4).strip()
    symbology = value_table.getValue(i, 5).strip()
    countOfFeatures = 0
    constraint_shape_type = tmpPathDesc.shapeType

    if notes is None:
        notes = "None"

    row_values = [((projectName, new_buildableID, constraintName, constraintPath,
                    buffMetric, buffType, notes, newProjectID,
                    buffDist, countOfFeatures, symbology))]

    # writing new features
    insert_cursor = arcpy.da.InsertCursor(sde_table_path,
                                          ['projectname', 'Buildable_Version', 'Constraint_Name', 'Constraint_Path',
                                           'Setback_Units', 'Setback_Type', 'Notes', 'PROJECT_ID',
                                           'SETBACK_DISTANCE', 'NUMBER_FEATURES', 'Symbology'])

    for row in row_values:
        insert_cursor.insertRow(row)

    del insert_cursor

arcpy.AddMessage("Added constraints to SDE Table!")
t1 = time.time()
arcpy.AddMessage(f"...   ... done in {datetime.timedelta(seconds=t1 - t0)}")

t0 = time.time()
arcpy.AddMessage("Going through constraint table values...")
# For loop to iterate through value table
for i in range(0, value_table.rowCount):
    # For loop to perform geometric operations
    constraintName = value_table.getValue(i, 0).strip()
    constraintName = constraintName.replace(' ', '_')
    tmpPath = value_table.getValue(i, 1).strip()
    tmpPathDesc = arcpy.Describe(tmpPath)
    fullPath = tmpPathDesc.catalogPath
    constraintPath = fullPath.replace("\\", "/")
    Dist = value_table.getValue(i, 2).strip()
    tmpDist = Dist.split()
    buffDist = tmpDist[0]
    buffMetric = tmpDist[1]
    buffType = value_table.getValue(i, 3).strip()
    notes = value_table.getValue(i, 4).strip()
    symbology = value_table.getValue(i, 5).strip()
    constraint_shape_type = tmpPathDesc.shapeType

    updateWhere = f"projectname = '{projectName}'" + "AND project_id = {} AND Buildable_Version = {}".format(
        newProjectID, new_buildableID) + f"AND Constraint_Name = '{constraintName}'"

    t0 = time.time()
    arcpy.AddMessage(f"{constraintName} is being queried...")
    # Loop through inputs if they are a feature layer
    if 'Server' in constraintPath:
        if 'MapServer' in constraintPath:
            service_layer = MapServiceLayer(constraintPath, gis=gis)
        elif 'FeatureServer' in constraintPath:
            service_layer = MapFeatureLayer(constraintPath, gis=gis)

        # Define the offset starting value
        offset = 0

        # Define the number of records to retrieve in each iteration
        limit = 2000

        # Create an empty dataframe to store the results
        result_df = pd.DataFrame()

        # Loop until all records have been retrieved
        while True:
            # Query the service with the offset and limit parameters
            tmpDF = service_layer.query(where='1=1', out_fields='*', geometry_filter=sa_filter,
                                        result_record_count=str(limit),
                                        as_df=True, offset=str(offset))

            # Append the results to the result dataframe
            result_df = result_df.append(tmpDF, ignore_index=True)

            # Check if the number of records retrieved is less than the limit
            if len(tmpDF) < limit:
                # If less, all records have been retrieved and break from the loop
                break

            # Update the offset for the next iteration
            offset += limit

        clipOUT = "in_memory/clip_{}".format(constraintName)
        buffOUT = "in_memory/buffer_{}".format(constraintName)

        # Include empty record into SDE
        if tmpDF.empty:
            arcpy.AddWarning("{0} is an empty dataset".format(constraintName))
            tmpFC = arcpy.CreateFeatureclass_management("in_memory", '{}'.format(constraintName))
            arcpy.RepairGeometry_management(tmpFC)
            feature_count = int(arcpy.GetCount_management('in_memory/{}'.format(constraintName)).getOutput(0))
            setbackGeomsAndFeatureCountDict[constraintPath] = feature_count
            buffDistNumeric = pd.to_numeric(buffDist)
            continue

        tmpFC = tmpDF.spatial.to_featureclass(outputGDB + '/' + 'Features' + '/' + '{}'.format(constraintName))

        arcpy.RepairGeometry_management(tmpFC)

        # select feature constraint by intersecting it with the aoi (boundary)
        test_features = arcpy.SelectLayerByLocation_management(in_layer=tmpFC, overlap_type='INTERSECT',
                                                               select_features=outaoiPath)

        t0 = time.time()
        arcpy.AddMessage("Writing GlobalID, Symbology, and ParentGlobalID")
        # for individual record in master table, write associated record to feature type table
        with arcpy.da.SearchCursor(sde_table_path, "GlobalID", where_clause=updateWhere) as cursor:
            for row in cursor:
                feature_global_id = row[0]

        # adding field for Symbology and updating it
        arcpy.AddField_management(tmpFC, 'Symbology', "TEXT", field_length=200)
        with arcpy.da.UpdateCursor(tmpFC, 'Symbology') as cursor:
            for row in cursor:
                row[0] = symbology
                cursor.updateRow(row)

        # adding field for Parent Global ID and updating it
        arcpy.AddField_management(tmpFC, 'ParentGlobalID', "Guid")
        with arcpy.da.UpdateCursor(tmpFC, 'ParentGlobalID') as cursor:
            for row in cursor:
                row[0] = feature_global_id
                cursor.updateRow(row)

        t1 = time.time()
        arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        tmp_fc_dissolve = "in_memory/fc_dissolve_service"

        t0 = time.time()
        if constraint_shape_type == 'Point':
            t0 = time.time()
            arcpy.AddMessage("Writing Point Feature to table...")
            arcpy.Dissolve_management(tmpFC, tmp_fc_dissolve, ['Symbology', 'ParentGlobalID'])
            arcpy.RepairGeometry_management(tmp_fc_dissolve)

            # append instead of using cursors
            arcpy.Append_management(tmpFC, constraint_tracker_points, schema_type='NO_TEST')
            t1 = time.time()
            arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        if constraint_shape_type == 'Polyline':
            t0 = time.time()
            arcpy.AddMessage("Writing Polyline Feature to table...")
            arcpy.Dissolve_management(tmpFC, tmp_fc_dissolve, ['Symbology', 'ParentGlobalID'], "", "MULTI_PART",
                                      "DISSOLVE_LINES")
            arcpy.RepairGeometry_management(tmp_fc_dissolve)

            arcpy.Append_management(tmpFC, constraint_tracker_lines, schema_type='NO_TEST')
            t1 = time.time()
            arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        if constraint_shape_type == 'Polygon':
            t0 = time.time()
            arcpy.AddMessage("Writing Polygon Feature to table...")
            # dissolve feature as list using buffer as an in_memory feature
            arcpy.Buffer_analysis(tmpFC, "in_memory/one_ft_fc_buffer", "1 Feet", "FULL", "", "LIST",
                                  ["ParentGlobalID", "Symbology"])
            arcpy.RepairGeometry_management("in_memory/one_ft_fc_buffer")

            arcpy.Dissolve_management("in_memory/one_ft_fc_buffer", tmp_fc_dissolve, ['Symbology', 'ParentGlobalID'])
            arcpy.RepairGeometry_management(tmp_fc_dissolve)

            arcpy.Append_management(tmp_fc_dissolve, constraint_tracker_polys, schema_type='NO_TEST')
            t1 = time.time()
            arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        # getting number of selected features of constraint
        selectedFeatures_Count = int(arcpy.GetCount_management(test_features).getOutput(0))

        tmpbufferPath = outputGDB + '/' + 'Buffers' + '/' + '{}_buffer_{}'.format(constraintName, buffDist)

        buffDistNumeric = pd.to_numeric(buffDist)

        # Preparing to buffer feature constraint and append to list of buffers that will be used for buildable land output
        if buffDistNumeric > 0:
            arcpy.AddMessage(f"Adding {constraintName} to list of buffers for buildable...")
            tmpBufferedSetback = arcpy.Buffer_analysis(tmpFC, buffOUT, Dist, buffType, "ROUND", "ALL", "", "PLANAR")
            arcpy.RepairGeometry_management(tmpBufferedSetback)
            arcpy.AddMessage(f"Buffering setback of {constraintName}...")
            arcpy.Buffer_analysis(tmpFC, tmpbufferPath, Dist, buffType, "ROUND", "ALL", "", "PLANAR")
            arcpy.RepairGeometry_management(tmpBufferedSetback)
            tmpbufferList.append(tmpBufferedSetback)
            arcpy.AddMessage("Finished {}".format(constraintName))

            setbackGeomsAndFeatureCountDict[constraintPath] = selectedFeatures_Count
        t1 = time.time()
        arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

    # If not looping through feature layer, then it must be reading a local layer
    else:
        t0 = time.time()
        arcpy.AddMessage("Clipping features to AOI")
        arcpy.env.extent = inAOI_buff_Extent
        clipOUT_user = 'in_memory/clip_{}'.format(constraintName)
        tmpFC = outputGDB + "/" + "Features" + "/"
        arcpy.Clip_analysis(constraintPath, outaoiPath, clipOUT_user)
        arcpy.RepairGeometry_management(clipOUT_user)
        # creating feature of constraint that will be written to sde
        tmp_fc2fc = arcpy.FeatureClassToFeatureClass_conversion(clipOUT_user, tmpFC, f'{constraintName}')
        tmp_fc_dissolve_local = "in_memory/fc_dissolve_local"
        arcpy.RepairGeometry_management(tmp_fc2fc)

        t1 = time.time()
        arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        t0 = time.time()
        arcpy.AddMessage("Writing GlobalID, Symbology, and ParentGlobalID")

        # getting fields and field names from feature of constraint
        fc_fields = arcpy.ListFields(tmp_fc2fc)
        fc_field_names = [f.name for f in fc_fields]

        with arcpy.da.SearchCursor(sde_table_path, "GlobalID", where_clause=updateWhere) as cursor:
            for row in cursor:
                feature_global_id = row[0]

        # Deleting Symbology if already in field names because it shouldn't be there
        if "Symbology" in fc_field_names:
            arcpy.AddMessage("Deleting existing Symbology field")
            arcpy.DeleteField_management(tmp_fc2fc, "Symbology")

        # adding field for Symbology and updating it
        arcpy.AddField_management(tmp_fc2fc, 'Symbology', "TEXT", field_length=200)
        with arcpy.da.UpdateCursor(tmp_fc2fc, 'Symbology') as cursor:
            for row in cursor:
                row[0] = symbology
                cursor.updateRow(row)

        # Deleting Parent Global ID if already in field names because it shouldn't be there
        if "ParentGlobalID" in fc_field_names:
            arcpy.DeleteField_management(tmp_fc2fc, "ParentGlobalID")

        # adding field for Parent Global ID and updating it
        arcpy.AddField_management(tmp_fc2fc, 'ParentGlobalID', "Guid")
        with arcpy.da.UpdateCursor(tmp_fc2fc, 'ParentGlobalID') as cursor:
            for row in cursor:
                row[0] = feature_global_id
                cursor.updateRow(row)

        t1 = time.time()
        arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        # begin writing features to sde as associated geometry types
        # polygons are the only feature that needs to have a temp 1 foot buffer
        if constraint_shape_type == 'Point':
            t0 = time.time()
            arcpy.AddMessage("Writing Point Feature to table...")

            arcpy.Dissolve_management(tmp_fc2fc, tmp_fc_dissolve_local, ['Symbology', 'ParentGlobalID'])
            arcpy.RepairGeometry_management(tmp_fc_dissolve_local)

            arcpy.Append_management(tmp_fc_dissolve_local, constraint_tracker_points, schema_type='NO_TEST')

            arcpy.AddMessage("Finished writing point features to Points Constraint Table!")

            t1 = time.time()
            arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        if constraint_shape_type == 'Polyline':
            t0 = time.time()
            arcpy.AddMessage("Writing Polyline Feature to table...")

            arcpy.Dissolve_management(tmp_fc2fc, tmp_fc_dissolve_local, ['Symbology', 'ParentGlobalID'], "",
                                      "MULTI_PART", "DISSOLVE_LINES")
            arcpy.RepairGeometry_management(tmp_fc_dissolve_local)

            arcpy.Append_management(tmp_fc2fc, constraint_tracker_lines, schema_type='NO_TEST')

            arcpy.AddMessage("Finished writing line features to Line Constraint Table!")

            t1 = time.time()
            arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        if constraint_shape_type == 'Polygon':
            t0 = time.time()
            arcpy.AddMessage("Writing Polygon Feature to table...")
            # dissolve feature as list using buffer as an in_memory feature
            arcpy.Buffer_analysis(tmp_fc2fc, "in_memory/one_ft_fc_buffer_2", "1 Feet", "FULL", "", "LIST",
                                  ["ParentGlobalID", "Symbology"])
            arcpy.RepairGeometry_management("in_memory/one_ft_fc_buffer_2")

            arcpy.Dissolve_management("in_memory/one_ft_fc_buffer_2", tmp_fc_dissolve_local,
                                      ['Symbology', 'ParentGlobalID'])
            arcpy.RepairGeometry_management(tmp_fc_dissolve_local)

            arcpy.Append_management(tmp_fc_dissolve_local, constraint_tracker_polys, schema_type='NO_TEST')

            arcpy.AddMessage("Finished writing polygon features to Polygon Constraint Table!")

            t1 = time.time()
            arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

        # get the number of features within the feature constraint
        t0 = time.time()
        arcpy.AddMessage("Checking if feature count is greater than 0 and then work into buildable area")
        feature_count = int(arcpy.GetCount_management(clipOUT_user).getOutput(0))
        setbackGeomsAndFeatureCountDict[constraintPath] = feature_count
        if feature_count == 0:
            arcpy.AddWarning("{0} has no features".format(constraintName))
        # add file path value to a new FilePath Field for the feature constraint
        arcpy.AddField_management(clipOUT_user, 'FilePath', "TEXT", field_length=200)
        fields = str(constraintPath)
        with arcpy.da.UpdateCursor(clipOUT_user, 'FilePath') as cursor:
            for row in cursor:
                row[0] = fields
                cursor.updateRow(row)

        # if there is more than 1 feature in feature constraint then add as a layer in the buildable land output
        if feature_count > 0:
            clipOUT_user = 'in_memory/clip_{}'.format(constraintName)
            arcpy.FeatureClassToFeatureClass_conversion(clipOUT_user, outputGDB + "/" + "Features" + "/",
                                                        f'{constraintName}')
            tmpBuffDist = int(str(Dist).split(" ")[0])
            buffOUTUser = outputGDB + "/" + "Buffers" + "/" + constraintName + "_Buffer_" + str(Dist).replace(' ', '')
            buffNoValue = outputGDB + "/" + "Buffers" + "/" + constraintName + "_NegativeBuffer"
            tmpDesc = arcpy.da.Describe(clipOUT_user)

            # if feature constraint is polygon and has 0 set as buffer distance then just append clipped polygon and no buffer so we can continue creating the buildable land output
            if tmpBuffDist == 0 and tmpDesc['shapeType'] == 'Polygon':
                arcpy.AddMessage("Zero POLYGON Buffer " + str(constraintName))
                bufferresultList.append(clipOUT_user)
                tmpbufferList.append(clipOUT_user)
            # otherwise the feature constraint must be a Point or Line so apply a temporary 1 foot buffer so we can continue creating the buildable land output
            elif tmpBuffDist == 0:
                arcpy.AddMessage("Zero POINT OR LINE Buffer " + str(constraintName))
                buffDist = '1 Feet'
                arcpy.Buffer_analysis(clipOUT_user, buffOUTUser, Dist, buffType, "ROUND", "ALL", "", "PLANAR")
                arcpy.AddField_management(buffOUTUser, 'FilePath', "TEXT", field_length=200)
                fields = str(constraintPath)
                with arcpy.da.UpdateCursor(buffOUTUser, 'FilePath') as cursor:
                    for row in cursor:
                        row[0] = fields
                        cursor.updateRow(row)
                tmpbufferList.append(buffOUTUser)
            # if the feature constraint has a buffer distance value then buffer like normal so we can continue creating the buildable land output
            elif tmpBuffDist > 0:
                arcpy.AddMessage("Buffer Setback..." + str(constraintName))
                arcpy.Buffer_analysis(clipOUT_user, buffOUTUser, Dist, buffType, "ROUND", "ALL", "", "PLANAR")
                arcpy.AddField_management(buffOUTUser, 'FilePath', "TEXT", field_length=200)
                fields = str(constraintPath)
                with arcpy.da.UpdateCursor(buffOUTUser, 'FilePath') as cursor:
                    for row in cursor:
                        row[0] = fields
                        cursor.updateRow(row)
                tmpbufferList.append(buffOUTUser)
            # if a negative buffer distance is applied then use that value so we can continue creating the buildable land output
            elif tmpBuffDist <= 0.0:
                arcpy.AddMessage("Negative Buffer " + str(constraintName))
                arcpy.Buffer_analysis(clipOUT_user, buffNoValue, Dist, buffType, "ROUND", "ALL", "", "PLANAR")
                arcpy.AddField_management(buffNoValue, 'FilePath', "TEXT", field_length=200)
                fields = str(constraintPath)
                with arcpy.da.UpdateCursor(buffNoValue, 'FilePath') as cursor:
                    for row in cursor:
                        row[0] = fields
                        cursor.updateRow(row)
                tmpbufferList.append(buffNoValue)
            else:
                arcpy.AddMessage("No Buffer Setback")
            arcpy.AddMessage("Finished Cycling Through " + str(constraintName))
        t1 = time.time()
        arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

t1 = time.time()
arcpy.AddMessage(f"Finished going through constraint table values... ...done in {datetime.timedelta(seconds=t1 - t0)}")

# Merging and dissolving buildable land
t0 = time.time()
arcpy.AddMessage("Merging and Dissolving Buildable Land")
arcpy.Merge_management(tmpbufferList, outputGDB + '/' + 'Buildable_Land' + '/Setbacks_merged')
arcpy.RepairGeometry_management(outputGDB + '/' + 'Buildable_Land' + '/Setbacks_merged')
arcpy.Dissolve_management(outputGDB + '/' + 'Buildable_Land' + '/Setbacks_merged',
                          outputGDB + '/' + 'Buildable_Land' + '/Setbacks_dissolved')
arcpy.RepairGeometry_management(outputGDB + '/' + 'Buildable_Land' + '/Setbacks_dissolved')
arcpy.AddMessage("Finished Merging and Dissolving Buildable Land")
t1 = time.time()
arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

t0 = time.time()
# Creating buildable land final output
arcpy.AddMessage("Creating Buildable Land Dataset")
unionOUT = outputGDB + "/" + "Buildable_Land" + "/" + "Buildable_Union"
buildableOUT = outputGDB + "/" + "Buildable_Land" + "/" + "Buildable_Land_Output"
arcpy.Dissolve_management(inAOI, outputGDB + '/' + 'Buildable_Land' + '/Dissolved_AOI', "", "", "MULTI_PART",
                          "DISSOLVE_LINES")
arcpy.RepairGeometry_management(outputGDB + '/' + 'Buildable_Land' + '/Dissolved_AOI')
arcpy.Union_analysis(
    [outputGDB + '/' + 'Buildable_Land' + '/Dissolved_AOI', outputGDB + '/' + 'Buildable_Land' + '/Setbacks_dissolved'],
    unionOUT)
arcpy.RepairGeometry_management(unionOUT)

with arcpy.da.UpdateCursor(unionOUT, "FID_Setbacks_dissolved") as cursor:
    for row in cursor:
        if row[0] == 1:
            cursor.deleteRow()

arcpy.Dissolve_management(unionOUT, buildableOUT)

dissolveFinalOut = outputGDB + "/" + "Buildable_Land" + "/" + f"BL_v{new_buildableID}_{project_gdb_name}_{datestr}"
arcpy.AddMessage(f"Buildable Out = {dissolveFinalOut}")
arcpy.AddMessage("Dissolving Buildable Land")
arcpy.Dissolve_management(buildableOUT, dissolveFinalOut, "", "", "MULTI_PART", "DISSOLVE_LINES")
arcpy.RepairGeometry_management(dissolveFinalOut)

buildable_poly_desc = arcpy.Describe(dissolveFinalOut)
buildable_poly_used = buildable_poly_desc.catalogPath

# creating acres field for buildable land output
arcpy.AddMessage("Getting acreage for Buildable Land")
buildable_poly_geometry = arcpy.CopyFeatures_management(buildable_poly_used, arcpy.Geometry())
buildable_poly_acres = buildable_poly_geometry[0].getArea("GEODESIC", "ACRES")

arcpy.AddField_management(dissolveFinalOut, "Area_ac", "DOUBLE", field_precision=10, field_scale=2)

with arcpy.da.UpdateCursor(dissolveFinalOut, "Area_ac") as cursor:
    for row in cursor:
        row[0] = buildable_poly_acres
        cursor.updateRow(row)

arcpy.AddMessage("Finished Dissolving")
arcpy.AddMessage("Finished Buildable Land Dataset")
t1 = time.time()
arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

# Update feature count in SDE Table
t0 = time.time()
arcpy.AddMessage("Updating feature count in SDE")
for i in range(0, value_table.rowCount):
    constraintName = value_table.getValue(i, 0).strip()
    constraintName = constraintName.replace(' ', '_')
    tmpPath = value_table.getValue(i, 1).strip()
    tmpPathDesc = arcpy.Describe(tmpPath)
    fullPath = tmpPathDesc.catalogPath
    constraintPath = fullPath.replace("\\", "/")
    countOfFeatures = setbackGeomsAndFeatureCountDict[constraintPath]

    updateWhere = f"projectname = '{projectName}'" + "AND project_id = {} AND Buildable_Version = {}".format(
        newProjectID, new_buildableID) + f"AND Constraint_Name = '{constraintName}'"

    with arcpy.da.UpdateCursor(sde_table_path, "NUMBER_FEATURES",
                               where_clause=updateWhere) as cursor:
        for row in cursor:
            row[0] = countOfFeatures
            cursor.updateRow(row)
t1 = time.time()
arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

t0 = time.time()
arcpy.AddMessage("Adding data from GDB")

# Add data from GDB to map
arcpy.env.workspace = outputGDB
aprx = arcpy.mp.ArcGISProject('CURRENT')
m = aprx.listMaps('*')[0]
layers = arcpy.ListDatasets(feature_type='feature')
constraints = "Features"
buildable = "Buildable_Land"
buildableOutput = f"BL_v{new_buildableID}_{project_gdb_name}_{datestr}"

for ds in layers:
    for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
        path = os.path.join(outputGDB, ds, fc)
        tmp_path = os.path.split(path)
        if tmp_path[0].endswith(buildable):
            if path.endswith(buildableOutput):
                m.addDataFromPath(path)
        elif tmp_path[0].endswith(constraints):
            m.addDataFromPath(path)

arcpy.AddMessage("Finished adding data from GDB.")
t1 = time.time()
arcpy.AddMessage(f"...   ... done in  {datetime.timedelta(seconds=t1 - t0)}")

arcpy.AddMessage("The GIS Gods are pleased")

