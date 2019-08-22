#Import Modules
import pandas as pd
import sys

#Get Input filename
file_name = sys.argv[1]
#Get Output filename
output_file = sys.argv[2]
#Get Schema + Table names
schema_table_name = sys.argv[3]

#Import csv file
input_csv = pd.read_csv(file_name,sep=",",encoding="utf-8")

#Lists to hold the datatype and lengths of each column
mydatatypes = []
mydatalengths = []

#Function to get the datatypes of each column in the CSV file
def getDataTypes():
    count = 0
    for type in input_csv.dtypes:
        if type == "bool":
            input_csv.iloc[:, count] = input_csv.iloc[:, count].replace({True:"Yes",False:"No"})
            type = "object"
        if type in ["int64","float64"]:
            mydatatypes.append("numeric")
            mydatalengths.append("")
        else:
            max_length = input_csv.iloc[:,count].str.len().max()
            mydatatypes.append("varchar2")
            mydatalengths.append(max_length)
        count += 1

#Function to create the CREATE statement
def getCreateStatement():
    create_statement = "CREATE TABLE " + schema_table_name + " ( \n"
    count = 0
    for i in input_csv.columns:
        if count == input_csv.shape[1] - 1:
            create_statement += str(i) + " " + str(mydatatypes[count])
            if mydatatypes[count] != "numeric":
                create_statement += "(" + str(mydatalengths[count]) + ")\n"
            else:
                create_statement += "\n"
        else:
            create_statement += str(i) + " " + str(mydatatypes[count])
            if mydatatypes[count] != "numeric":
                create_statement += "(" + str(mydatalengths[count]) + "),\n"
            else:
                create_statement += ",\n"
        count += 1
    create_statement += ");"

    with open(output_file,"w") as f:
        f.write(create_statement + "\n")

#Function to create the INSERT statements
def getInsertStatement():
    temp = ""
    for j in range(0,input_csv.shape[0]):
        insert_statement = "insert into " + schema_table_name + " values ("
        for j1 in range(0,input_csv.shape[1]):
            if j1 == input_csv.shape[1] - 1:
                if mydatatypes[j1] != "numeric":
                    temp += "'" + str(input_csv.iloc[j, j1]) + "'"
                else:
                    temp += str(input_csv.iloc[j, j1])
            else:
                if mydatatypes[j1] != "numeric":
                    temp += "'" + str(input_csv.iloc[j,j1]) + "',"
                else:
                    temp += str(input_csv.iloc[j,j1]) + ","
        insert_statement += temp + ");"
        with open(output_file,"a") as f:
            f.write(insert_statement+"\n")
        temp = ""
        insert_statement = ""

#Main Function
def main():
    getDataTypes()
    getCreateStatement()
    getInsertStatement()
    with open(output_file,"a") as f:
        f.write("COMMIT;")

if __name__ == "__main__":
    main()