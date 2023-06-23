import pandas as pd
import json
import numpy as np

def flatten_json(obj):
    ret={}
    
    def flatten(x,flatten_key=""):
        if type(x) is list:
            i=0
            for elem in x:
                flatten(elem,flatten_key + str(i)+'_')
                i+=1
        elif type(x) is dict:
             for current_key in x:
                 flatten(x[current_key],flatten_key + current_key +'_')
        else:
            ret[flatten_key]=x          
    flatten(obj)        
    return ret

# data_file=input("Enter the file to flatten")
# configg_file=input("Enter the config file name")
with open(f'/workspaces/FHIR_Framework/Input_data_files/org_aff.json') as data:
    data_file=json.load(data)   
    nested_obj=data_file     
# data.close()

if __name__ =="__main__":
    
    ans=flatten_json(nested_obj)
    with open('output.json', 'w') as f:
        json.dump(ans, f)
    
    with open(f'/workspaces/FHIR_Framework/Config_method2/gen_config.json') as config_file:
        con=json.load(config_file)
    # config_file.close()
    nextlevelname=''
    q=1
    tables=con['Tables']
    df={}
    for i in range(1,len(tables)+1):
       
        df[i]=pd.DataFrame()
        newtable=tables[f"table{i}"]
        thatlevelname=newtable[f'L{q}_array_name']
        if(f'L{q+1}_array_name' in newtable):
            nextlevelname=newtable[f'L{q+1}_array_name']    
        for key,value in newtable.items():
           

            
            if key=="column_names":
                
                for elem in value:
                  
                    
                  df[i][f'{thatlevelname}_{elem}']=np.nan
                  for j in range(len(ans)):
                    
                    if(nextlevelname==elem.split('_')[0]):
                      
                      for t in range(len(ans)):  
                        ans_key=f"{thatlevelname}_{j}_{nextlevelname}_{t}_{elem.split('_')[1]}_"
                        if(ans_key in ans):      
                            df[i].loc[j,f"{thatlevelname}_{elem}"]=ans[ans_key]
                    else: 
                         
                        ans_key=f"{thatlevelname}_{j}_{elem}_"
                   
                        if(ans_key in ans):      
                            df[i].loc[j,f"{thatlevelname}_{elem}"]=ans[ans_key]    
                    
                
                

            elif "L" not in key and "_array_name" not in key and key!="No_of_array_levels":
                    
                    print(key)
                    print(ans[f'{newtable[key]}_'])
                    df[i][f'{newtable[key]}']=[ans[f'{newtable[key]}_']]
    
        df[i].to_csv(f'output_{i}.csv')
