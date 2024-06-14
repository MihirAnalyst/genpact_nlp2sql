
from flask import Flask, Response, jsonify, make_response, redirect,render_template, request, session
import json
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import create_engine,inspect,text
from urllib.parse import quote_plus
import json
import pandas as pd
# from langchain.chat_models import ChatOpenAI
from langchain_community.chat_models import ChatOpenAI
import os
import plotly.io as pio
import plotly.graph_objects as go
import pymssql


master_session={}
app = Flask(__name__)
app.secret_key = '123456'


# Configure Redis for session storage
# app.config['SESSION_TYPE'] = 'redis'
# app.config['SESSION_PERMANENT'] = False
# app.config['SESSION_USE_SIGNER'] = True
# app.config['SESSION_KEY_PREFIX'] = 'session:'

os.environ["OPENAI_API_KEY"] = ""
llm_model = "gpt-3.5-turbo"
llm = ChatOpenAI(temperature=0.1, model=llm_model)

with open("table_structure.txt", 'r') as file:
    table_struct = file.read()

with open("example.txt", 'r') as file:
    example_st = file.read()

with open("prompt.txt", 'r') as file:
    instructions = file.read()


@app.route('/')
@app.route('/login')
def login():
    print('login called ----------------')
    print('login',session)
    return render_template('login/login.html')


@app.route('/verifylogin', methods=['POST'])
def verifylogin():
    print('verifylogin called ----------------')
    if request.method == 'POST':
        username=request.form['username']
        password=request.form['password']
    with open('users.json', 'r') as f:
        users_data = json.load(f)
        if username in users_data and users_data[username] == password:
            data={'msg':'success','user':'true','password':'false'}  
            session['user']=username
            session[username]={'metadata':{}}
            print('verify login',session)
            return data
        else:
            if username in users_data:
                data={'msg':'error','user':'right','password':'wrong'}   
                return data
            else :
                data={'msg':'error','user':'wrong','password':'wrong'}
                return data

@app.route('/logout')
def logout():
    print('logout called ----------------')
    del session[session['user']]
    del session['user']
    print(session)
    return redirect('/')

@app.route('/disconnect',methods=['GET'])
def disconnect():
    print('disconnect called ----------------')
    try:
        session[session['user']]={"metadata":{}}
        print(session)
        return 'success'
    except Exception as e :
        print(e)
        return 'error'


@app.route('/main')
def main1():
    print('main1 called ----------------')
    print('vvvv',session)
    return render_template('index.html')


@app.route('/getquery',methods=['POST'])
def getquery():
    print('getquery called ----------------')
    try:
        if request.method=='POST':
            query=request.form['qry']
            print(query)
            html_table,graph_html,sqlquery= main(query)
            return jsonify({"table":html_table,"msg":"success","graph":graph_html,"query":sqlquery})
    except Exception as e:
        print(e)
        return jsonify({"msg":'error'})



@app.route('/connectdb' ,methods=['POST'])
def conectdb():
    print('conectdb called ----------------')
    try:
        if (request.method == 'POST'):
            db_host=request.form['hostname']
            db_user=request.form['user']
            db_password=request.form['password']
            db_port=request.form['portno']
            db_name=request.form['database']   
            conn, connection_string, engine,mastertbl=connectmysqldb(db_user,db_password,db_host,db_port,db_name)
            print("12345",session)
            print(db_user,db_password,db_host,db_port,db_name)
            # master_session[session['user']]['conn']=conn
            session[session['user']]={'metadata':{ 
                                                    "db_host":request.form['hostname'], 
                                                    "db_user":request.form['user'],
                                                    "db_password":request.form['password'],
                                                    "db_port":request.form['portno'],
                                                    "db_name":request.form['database'],
                                                     "schema":"{}"
                                                    }}
            print(session)
            return  jsonify({"msg":"success","schema":mastertbl})
    except Exception as e:
        print(e)
        return jsonify({"msg":'error'})

@app.route('/getmetadata' ,methods=['GET'])
def getmetadata():
    print('getmetadata called ----------------')
    print(session)
    try:
        if (request.method == 'GET'):
            value = session[session['user']].get('metadata') 
            print('getmetadata',value)
            print('getdata',session)
            if value :
                db_user=session[session['user']]['metadata']['db_user']
                db_password=session[session['user']]['metadata']['db_password']
                db_host=session[session['user']]['metadata']['db_host']
                db_port=session[session['user']]['metadata']['db_port']
                db_name=session[session['user']]['metadata']['db_name']
                conn, connection_string, engine,schema=connectmysqldb(db_user,db_password,db_host,db_port,db_name)
                # master_session[session['user']]['conn']=conn
                return jsonify({"metadata":session[session['user']]['metadata'],"schema":schema})
            else :
                return 'nothing'
    except Exception as e:
        print(e)
        return 'nothing'



def get_databases(engine):
    print('get_databases called ----------------')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sys.databases"))
        return [row[0] for row in result]
    
def connectmysqldb(db_user, db_password, db_host, db_port, db_name):
    print('connectmysqldb called ----------------')
    database_structure = {}
    encoded_password = quote_plus(db_password)
    connection_string = f'mssql+pymssql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
    print(connection_string)
    engine = create_engine(connection_string)
    conn = engine.connect()
    print("This is conn", conn)
    database_structure[db_name] = {}
    # print(f"Database: {db_name}")
    inspector_db = inspect(engine)
    # Get list of schemas
    schemas = get_schemas(inspector_db)
    for schema in schemas:
            database_structure[db_name][schema] = {'tables': {}, 'views': {}}
            # print(f"  Schema: {schema}")

            # Get tables and views for each schema
            tables, views = get_tables_and_views(inspector_db, schema)
            # print(f"    Tables in {schema}:")
            for table in tables:
                # print(f"      {table}")
                # Get columns for each table
                columns = get_columns(inspector_db,schema,table)
                if columns:
                    database_structure[db_name][schema]['tables'][table] = {column['name']: str(column['type']) for column in columns}

            # print(f"    Views in {schema}:")
            for view in views:
                # print(f"      {view}")
                # Get columns for each view
                columns = get_columns(inspector_db, schema, view)
                if columns:
                    database_structure[db_name][schema]['views'][view] = {column['name']: str(column['type']) for column in columns}
    # engine.dispose()
    return conn, connection_string, engine ,database_structure

def get_schemas(inspector):
    # print('get_schemas called ----------------')
    return inspector.get_schema_names()
    

 
def get_tables_and_views(inspector, schema):
    # print('get_tables_and_views called ----------------')
    tables = inspector.get_table_names(schema=schema)
    views = inspector.get_view_names(schema=schema)
    return tables, views
 
 
def get_columns(inspector, schema, table_name):
    # print('get_columns called ----------------')
    try:
        return inspector.get_columns(table_name, schema=schema)
    except NoSuchTableError:
        return []
    



def get_table_names(data):
    print('get_table_names called ----------------')
    table_names = []
    for db_name, db_content in data.items():
        for schema_name, schema_content in db_content.items():
            for table_or_view, tables_and_views in schema_content.items():
                if table_or_view == 'tables':
                    for table_name in tables_and_views:
                        table_names.append(table_name)
    return table_names

@app.route('/generatedescription' ,methods=['POST'])
def gendescription():
    try:
        print('gendescription called ----------------')
        if (request.method == 'POST'):
            structure=request.form['schema']
            #print(type(structure))
            print('\n\nStructure:',structure)
            #print(session)
            conn=get_connection_pymysql()
            #tablelist=['customers','order_items','orders','products']
            schema=getTableSchema(conn,json.loads(structure))
            print('\n\nSchema:\n',schema)
            session[session['user']]['metadata']['table_desc']=schema
            session[session['user']]['metadata']['schema']=structure #schema
            session.modified = True
            #print('\n\nStored Session:',session)    
            return jsonify({"msg":'success',"metadata":session[session['user']]['metadata']})
    except Exception as e :
        print(e)
        return jsonify({"msg":'error',"metadata":session[session['user']]['metadata']})


def get_connection():
    print('get_connection called ----------------')
    db_user=session[session['user']]['metadata']['db_user']
    db_password=session[session['user']]['metadata']['db_password']
    db_host=session[session['user']]['metadata']['db_host']
    db_port=session[session['user']]['metadata']['db_port']
    db_name=session[session['user']]['metadata']['db_name']
    encoded_password = quote_plus(db_password)
    connection_string = f'mssql+pymssql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
    print(connection_string)
    engine = create_engine(connection_string)
    conn = engine.connect()
    #conn.execute
    return conn

def get_connection_pymysql():
    print('get_connection_pymysql called ----------------')
    db_user=session[session['user']]['metadata']['db_user']
    db_password=session[session['user']]['metadata']['db_password']
    db_host=session[session['user']]['metadata']['db_host']
    db_port=session[session['user']]['metadata']['db_port']
    db_name=session[session['user']]['metadata']['db_name']
    encoded_password = quote_plus(db_password)
    conn = pymssql.connect(database=db_name,host=db_host,user=db_user,password=db_password,as_dict=True,port=db_port)
    print('End get_connection_pymysql')
    return conn

def getTableSchema(conn,structure):
    print('getTableSchema called----------------')
    print(structure)
    db_name=session[session['user']]['metadata']['db_name']
    #tablelist=list(structure[db_name]['dbo']['tables'].keys())
    #print('getTableSchema called----------------')
    schema_defn={}
    for folder in list(structure[db_name].keys()):
        for sub_folder in list(structure[db_name][folder].keys()):
            tablelist=list(structure[db_name][folder][sub_folder].keys())
            print('Table _list:',tablelist)
            for i in tablelist:
                print('Table: ',i)
                column_list=list(structure[db_name][folder][sub_folder][i].keys())
                print(column_list)
                cursor=conn.cursor()
                cursor.execute(f"select COLUMN_NAME, DATA_TYPE, table_schema from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME= '{i}'")
                columnnames2=cursor.fetchall()
                columnnames=[]
                
                #Code to Filter Columns
                for col in columnnames2:
                    if col['COLUMN_NAME'] in column_list:
                        columnnames.append(col)
                        
        
                print('Column Names:', columnnames)
                schemaname=columnnames[0]['table_schema']
                cursor=conn.cursor()
                
                #Code to select 3 examples of selected rows
                temp_str=''
                for column in column_list:
                    temp_str+=column
                    temp_str+=','
                temp_str=temp_str[:-1]
                temp_str
                cursor.execute(f'Select top 3 {temp_str} from {i}')
                columnval=cursor.fetchall()
                #print('Column Names:', columnval)
                column_details={}
                
                # SATYAJIT CODE- SENDS EXAMPLES
                # for j in columnnames:
                #     column_details[j['COLUMN_NAME']]=[j['DATA_TYPE'],columnval[0][j['COLUMN_NAME']],columnval[1][j['COLUMN_NAME']],columnval[2][j['COLUMN_NAME']]]
                
                # MIHIR CODE- NO EXAMPLES
                for j in columnnames:
                    column_details[j['COLUMN_NAME']]=j['DATA_TYPE']
                
                schema_defn[schemaname+'.'+i]=column_details
    cursor.close()
    return schema_defn

def main(nlquestion):
    print('main called ----------------')
    conn=get_connection()
    # prompt_sql = f"""Based on the table schema, write a clean MS SQL query(without mentioning sql in the begining) that would answer the user's question. Use the TOP function instead of LIMIT if it is needed.
    # The format of the schema is in json format {{"Table Name":{{"Column Name":['data type','value 1',value 2', 'value 3']}}}}
    
    prompt_sql = f"""Based on the table schema, write a clean MS SQL query(without mentioning sql in the begining) that would answer the user's question. Use the TOP function instead of LIMIT if it is needed.
    The format of the schema is in json format {{"Table Name":{{"Column Name":'data type'}}}}

    

    TABLE DESCRIPTION:
    {session[session['user']]['metadata']['table_desc']}

    QUESTION:
    {nlquestion}

    ANSWER:

"""
    Sql_ans = llm.invoke(prompt_sql)
    print('\n\n\n\n\n----------------------------',prompt_sql)
    ans = Sql_ans.content.replace("\n"," ").replace("Answer","").replace(":"," ").replace('"','')
    # print(ans)
    df = pd.read_sql(text(ans), conn)
    df.to_csv('Test_data.csv',index=False)
    newdf= df.to_html(index=False)
    # print(df)
    df_structure = {col: str(df[col].dtype) for col in df.columns}
    with open("graph_prompt.txt", 'r') as file:
        g_prompt = file.read()

    graph_prompt = f"""{g_prompt}\

    Database Structure:
    {df_structure}

    OUTPUT:

    """
    print('\n\nGRAPH PROMPT: \n\n',graph_prompt)
    Sql_graph = llm.invoke(graph_prompt)
    
    img_code = Sql_graph.content.replace("python","").replace("`","")

    img_code+='\nprint(a)'

    print('\n\nSql_graph Code: ',img_code)
    print('\n_____________________________________________________________')
    #graph = {}
    #print(img_code)
    img_code+='\ngraph_object=analyze_g(data)'
    compile_exec = {'data':df}
    
    conn.close()

    try:
        exec(img_code, compile_exec)
        print('\n**********************Code Executed.*******************\n')
        g1 = compile_exec
        new_graph = pio.to_html(g1['graph_object'], full_html=False,include_plotlyjs=False,config={'responsive': True},default_height="338px",default_width="100%",div_id="mygraph")
    
        return(newdf,new_graph,ans)
    except Exception as e:
        print('Graph Code Execution Failed:\n',e)
        return(newdf,None,ans)

if __name__ == '__main__':
    app.run(debug=True)
