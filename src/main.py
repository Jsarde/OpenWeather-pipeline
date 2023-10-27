from dotenv import load_dotenv
import requests
import os
from datetime import datetime
import pandas as pd


class OpenWeather:
    def __init__(self,city: str) -> None:
        load_dotenv()
        api_key = os.getenv("API_KEY")
        self.url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metrics"
    
    def __str__(self) -> str:
        explanation = "This class returns the current weather of a given city using OpenWeather API"
        return explanation
    
    def __status(self,resp: requests.Response):
        flag = False
        code = resp.status_code
        if code == 200:
            flag = True
            message = "OK"
        elif code == 401:
            message = f"{code} - API Key error"
        elif code == 404:
            message = f"{code} - City error"
        elif code == 429:
            message = f"{code} - API call limit exceeded"
        else:
            message = f"{code} - other error"
        return flag, message
    
    def __kelvin_to_fahrenheit(self,kelvin: float) -> float:
        fahrenheit = (kelvin - 273.15) * (9/5) + 32
        return fahrenheit
    
    def __processing(self,data: dict) -> pd.DataFrame:
        info = {
            "city" : data["name"],
            "country" : data["sys"]["country"],
            "lat" : data["coord"]["lat"],
            "lng" : data["coord"]["lon"],
            "weather" : data["weather"][0]["description"],
            "temperature (F°)" : self.__kelvin_to_fahrenheit(data["main"]["temp"]),
            "humidity" : data["main"]["humidity"],
            "wind_speed" : data["wind"]["speed"],
            "cloudiness (%)" : data["clouds"]["all"],
            "time" : datetime.utcfromtimestamp(data["dt"] + data["timezone"])
        }
        tempdf = pd.DataFrame([info])
        return tempdf
    
    def forecast(self):
        try:
            response = requests.get(self.url)
        except Exception as e:
            print(f"Request error >> {str(e)}")
        else:
            flag, message = self.__status(resp=response)
            if flag:
                df = self.__processing(response.json())
                return df
            else:
                return message



if __name__ == "__main__":
    input_city = input("Insert city's name >>: ")
    weather = OpenWeather(city=input_city)
    result = weather.forecast()
    print(result)