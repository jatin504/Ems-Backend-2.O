from app.utils.db_utils import DB
from datetime import datetime,timedelta
from device_config_helper import DeviceConfigHelper


async def fetch_battery_bank_data(plant_number):

        final_data  = []
        
        async with DB() as session:
            result = await session.fetchrow("SELECT * from plant_device_summary where plant_id = $1 and is_active = true ",plant_number)

            if result == None:
                return []
            
            from_date = datetime.now().date()
            to_date = datetime.now().date() + timedelta(days=1)

            for bank in result['bank_list']:

                data_1 = await session.fetchrow("select time_stamp ,temperature , voltage ,amps  ,soc  from " +bank +"  where time_stamp >= $1 and  time_stamp < $2  order by time_stamp desc  limit 1",from_date ,to_date )
                if data_1 == None:
                    data_1 = {'time_stamp':'','temperature':'','voltage':'','amps':'','soc':''}
               
                data_2 = await session.fetchrow("select max(temperature ) as max_temperature from " +bank +" where time_stamp >= $1 and  time_stamp < $2 ",from_date ,to_date)
                if data_2 == None:
                    data_2 = {'max_temperature':''}
                active_device = 0
                non_active_device =0
                
                for rack in result['rack_list']:
                    new_bank = bank.replace('ess_','',1)
                    if rack.startswith(new_bank) :
                        data_3 = await session.fetchrow("select * from "+ bank+" where time_stamp >= $1 and  time_stamp < $2 ",from_date ,to_date)
                       
                        if data_3 != None:
                            active_device = active_device +1
                        else:
                            non_active_device = non_active_device +1
                final_data.append({'device_id':bank,'time_stamp':data_1['time_stamp'],'latest_temp':data_1['temperature'],'voltage':data_1['voltage'],'amps':data_1['amps'],'soc':data_1['soc'],'max_temp':data_2['max_temperature'],'active_device':active_device,'non_active_device':non_active_device})

        return final_data


async def fetch_battery_bank_data_v2(plant_number):

        final_data = []

        async with DB() as session:
            helper = DeviceConfigHelper() 
            result = helper.get_device_ids_by_type_pattern("bank_bank") or [] # temp added for auto bank device

            if not result:
                return []

            from_date = datetime.now().date()
            to_date = datetime.now().date() + timedelta(days=1)

            racks_by_bank = {}
            rack_list = helper.get_device_ids_by_type_pattern("rack_rack")
            for bank in result:
                racks_by_bank[bank] = [] 
                for rack in rack_list:
                    new_bank = bank.replace("ess_", "", 1)
                    if rack.startswith(new_bank):
                        racks_by_bank[bank].extend([rack])
            


            for bank in result:
                data_1 = await session.fetchrow(f""" WITH rows as ( 
                                                      SELECT time_stamp ,battery_cabin_temperature_1 , phase_a_voltage ,total_output_current_of_rectifiers  ,soc , max(battery_cabin_temperature_1 )  OVER() as max_temperature  
                                                            ,row_number() OVER() as rn from {bank} where time_stamp >= $1 AND time_stamp < $2 ) 
                                                            select * from rows where rn =1;
                                                """,from_date,to_date)
                if data_1 == None:
                    final_data.append(
                    {
                        "device_id": bank,
                        "time_stamp": "",
                        "battery_cabin_temperature_1": "",
                        "phase_a_voltage": "",
                        "total_output_current_of_rectifiers": "",
                        "soc": "",
                        "max_temp": "",
                        "active_device": "",
                        "non_active_device": "",
                    }
                    )
                    continue

                row = {'active_device':0,'non_active_device':0}
                
                if racks_by_bank.get(bank,[]) != []:
                    q_parts = []
                    for racks in racks_by_bank.get(bank):
                        q_parts.append(
                            f"select exists (select 1 from {racks} WHERE time_stamp >= $1 AND time_stamp < $2 LIMIT 1) AS online"
                        )

                    union_sql = "\n UNION ALL \n ".join(q_parts)

                    sql = f"""
                            select count(*) FILTER (WHERE online) as active_device, count(*) FILTER (WHERE NOT online) as non_active_device from ( {union_sql})
                            """
                    row = await session.fetchrow(sql,from_date,to_date)

                final_data.append(
                    {
                        "device_id": bank,
                        "time_stamp": data_1["time_stamp"],
                        "latest_temp": data_1["battery_cabin_temperature_1"],
                        "voltage": data_1["phase_a_voltage"],
                        "amps": data_1["total_output_current_of_rectifiers"],
                        "soc": data_1["soc"],
                        "max_temp": data_1["max_temperature"],
                        "active_device": row['active_device'],
                        "non_active_device": row['non_active_device'],
                    }
                )

        return final_data