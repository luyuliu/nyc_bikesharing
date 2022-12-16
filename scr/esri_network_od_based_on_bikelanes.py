# This file transform the floorplans into centerline, then into network, and calculate OD based on the provided room centers.
# This is the 1ST file you need to run.

import multiprocessing, sys
import arcpy, csv, numpy, json, time
from tqdm import tqdm
from pymongo import MongoClient
client = MongoClient("localhost:27017")

def construct_nextwork(timestamp):
    arcpy.env.overwriteOutput = 1 # Overwrite the output is important, to avoid mistakes.
    arcpy.env.parallelProcessingFactor = "75%"
    arcpy.CheckOutExtension('Foundation') # Register the extensions
    arcpy.CheckOutExtension('Network')
    basePath = r"D:\Luyu\nyc_bikesharing\\"
    geodatabasesPath = basePath + "network_data_vehicle\\"
    sr = 'PROJCS["NAD_1983_StatePlane_New_York_Long_Island_FIPS_3104",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",300000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-74.0],PARAMETER["Standard_Parallel_1",40.66666666666666],PARAMETER["Standard_Parallel_2",41.03333333333333],PARAMETER["Latitude_Of_Origin",40.16666666666666],UNIT["Meter",1.0]]'
    networkName = "nyc_vehicles"
    sidewalkLocation = basePath + "nyc_bikesharing.gdb\\" + networkName

    arcpy.CreateFileGDB_management(geodatabasesPath, "OD")
    geodatabasePath = geodatabasesPath + "OD" + ".gdb\\"
    arcpy.env.workspace = geodatabasePath

    # Build network dataset
    arcpy.CreateFeatureDataset_management(geodatabasePath, "network", sr)
    arcpy.FeatureClassToGeodatabase_conversion(sidewalkLocation, geodatabasePath + "//network")
    arcpy.Rename_management("network//" + networkName, "raw_network_layer")
    # arcpy.na.CreateNetworkDataset(geodatabasePath + "//network", "raw_network_layer", "network//raw_network_layer", "ELEVATION_FIELDS")
    arcpy.CreateNetworkDatasetFromTemplate_na(basePath + "//schema.xml", geodatabasePath + "//network")
    network = arcpy.na.BuildNetwork(geodatabasePath + "//network//raw_network_layer")

    return False
    # Create stops as Origin and Destination layers in OD analysis layer
    csvFile = geodatabasesPath + "stops.csv"
    wgs84 = r'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
    stops_wgs = arcpy.management.MakeXYEventLayer(csvFile, "XLON", "XLAT", "stops_wgs", wgs84)
    arcpy.management.Project(stops_wgs, "stops", sr)
    arcpy.AddField_management("stops", "Name", "TEXT", field_length=80)
    arcpy.CalculateField_management("stops", "Name", "!Station!", "PYTHON_9.3")

    # Make OD layer: create OD analysis layer
    ODLayer = arcpy.na.MakeODCostMatrixLayer(network, "OD", "Length")
    origins = geodatabasePath + "//stops"
    destinations = geodatabasePath + "//stops"
    arcpy.na.AddLocations(ODLayer, "Origins", origins)
    arcpy.na.AddLocations(ODLayer, "Destinations", destinations)
    result = arcpy.na.Solve(ODLayer)
    # arcpy.na.GenerateOriginDestinationCostMatrix(origins, destinations, geodatabasePath + "//network//raw_network_layer", geodatabasePath)
    lineLayer = ODLayer.getOutput(0) # 3 is the default order number of the od result layer in the layer collection.
    # ODLayerOutput = arcpy.SaveToLayerFile_management(lineLayer, "\OD_layers\OD_layers_" + str(timestamp), "RELATIVE")
    lineLayer = (lineLayer.listLayers())[3]

    # Output OD in CSV

    arcpy.conversion.TableToTable(lineLayer, geodatabasesPath + "OD_results", "raw_OD.csv") # This is the raw OD matrix, which does not have the right field name and SPACEID for the origin and destination rooms
    header = ["ORIGINNAME", "DESTINATIONNAME", "ORIGINID", "DESTINATIONID", "DESTINATIONRANK", "LENGTH"]
    with open(geodatabasesPath + "OD_results\\" + "raw_OD.csv") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        insertRows = []
        for row in readCSV:
            insertRow = []
            Name = row[1]
            if Name == "Name":
                continue
            nameList = Name.split(" - ") # Split the name field in the raw OD matrix. Its structure is like: 089-02-aaaa - 089-02-bbbb. So we can separate them
            ORIGINNAME = nameList[0]
            DESTINATIONNAME = nameList[1]
            
            insertRow = [ORIGINNAME, DESTINATIONNAME, row[2], row[3], row[4], row[5]]
            insertRows.append(insertRow)

    with open(geodatabasesPath + "OD_results\\" + "OD.csv", 'w', newline='') as afile: # Output the record-based od matrix, you will need to run transformation.py to transform it from record-based to matrix-based.
        writer = csv.writer(afile)
        writer.writerow(header)
        for i in insertRows:
            writer.writerow(i)

if __name__ == '__main__':
    timestamp = ""
    a = time.time()
    construct_nextwork(timestamp)
    b = time.time()
    print("Time: ", timestamp, " --- ", b-a, " seconds.")
