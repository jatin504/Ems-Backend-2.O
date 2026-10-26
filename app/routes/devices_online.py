from quart import Blueprint, request, jsonify
from device_config_helper import DeviceConfigHelper
from app.services.latest_services import get_latest_data_from_devices
from app.utils.db_utils import DB

from datetime import datetime, timedelta
from app.utils.constant import SUCCECS_CODE, ERROR_CODE
import logging

logger = logging.getLogger(__name__)

online_devices_bp = Blueprint("online_devices_bp", __name__)


@online_devices_bp.route('/api/online_devices',methods=['POST'])
async def online_devices():
        try:
            data = await request.get_json()
            pattern = data.get('pattern','')
            column_name = data.get('column','active_power')

            helper = DeviceConfigHelper()
            device_list = helper.get_device_ids_by_type_pattern(pattern=pattern)
            async with DB() as session:
                online_device,stop ,active,inactive= 0,0,0,0
                
                for device in device_list:
                  
                    data = await session.fetchrow("select time_stamp,"+ column_name+"  from "+device +" order by time_stamp desc limit 1")
                    if data[column_name] == 0:
                        stop =stop+1
                    else :
                        online_device = online_device +1

                    if data['time_stamp'] < datetime.now() and data['time_stamp'] > (datetime.now() - timedelta(minutes=5)):
                        active = active +1
                    else:
                        inactive = inactive + 1

            return {'response':SUCCECS_CODE,'data':{'device_list':device_list,'online':online_device,'stop':stop,'active':active,'inactive':inactive}}
        except Exception as e:
            print(e)
            return {"response":ERROR_CODE,'message':"Column may be incorrect"}