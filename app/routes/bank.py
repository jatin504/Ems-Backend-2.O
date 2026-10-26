# from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy import text
from datetime import datetime,timedelta
from app.utils import constant
from app.utils.db_utils import DB

class Service:
    def  __init__(self):
        print("starting services")
        self.__db_url = {'dsn':'postgresql://postgres:postgres@localhost:5432/myplantdb',"min_size":2,"max_size":10}
        # self.__engine = create_async_engine(self.__db_url, echo=False, pool_size=10, max_overflow=20)
        # self.__session = sessionmaker(self.__engine, class_=AsyncSession, expire_on_commit=False)
        

    async def fetch_pcs_data(self,plant_number):

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