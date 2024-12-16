from google.transit import gtfs_realtime_pb2
import requests
import re
from datetime import datetime, timezone
from constants import D_LINE_STOPS

def is_northbound_d_train(trip_update):
    is_d_train = trip_update.trip.route_id == "D"
    is_northbound = parse_trip_direction(trip_update.trip.trip_id) == "North"

    return is_d_train and is_northbound

def parse_trip_direction(trip_id):
    pattern = r"^\d{6}_D\.\.(S|N)"
    match = re.match(pattern, trip_id)

    if match:
        direction = match.group(1)
        return "South" if direction == "S" else "North"
    return None

def get_future_arrivals(stop: str):
    URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm"

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(URL)
    feed.ParseFromString(response.content)
    future_arrivals = {}
    
    for entity in feed.entity:    
        if entity.HasField("trip_update") and is_northbound_d_train(entity.trip_update):
            trip_id = entity.trip_update.trip.trip_id
            stop_time_updates = entity.trip_update.stop_time_update

            stop_arrival_time = None
            last_stop = None

            for update in stop_time_updates:
                if D_LINE_STOPS[update.stop_id] == stop:
                    stop_arrival_time = update.arrival.time

            if stop_arrival_time:
                last_stop = D_LINE_STOPS[stop_time_updates[-1].stop_id]

            if last_stop:
                future_arrivals[trip_id] = {
                    "arrival_time": stop_arrival_time,
                    "last_stop": last_stop
                }
                
    return future_arrivals

if __name__ == "__main__":

    stop = "79 St"
    future_arrivals = get_future_arrivals(stop)
    current_time = datetime.now(tz=timezone.utc)
    for train, arrival in future_arrivals.items(): 
        arrival_time = arrival_unix_time = datetime.fromtimestamp(arrival["arrival_time"], tz=timezone.utc)
        difference_in_minutes = int((arrival_time - current_time).total_seconds() // 60)

        print(f"D {arrival["last_stop"]} arriving to {stop} in {difference_in_minutes} minutes")
