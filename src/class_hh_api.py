from abc import ABC, abstractmethod
from typing import Any, Dict, List

import requests
from requests import Response


class GetApi(ABC):
    """Абстрактный класс"""

    @abstractmethod
    def get_response(self, *args: Any, **kwargs: Any) -> Response:
        """Абстрактный метод запроса к Api"""
        pass

    @abstractmethod
    def get_vacancies_response(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """Абстрактный метод получения вакансий"""
        pass

    @abstractmethod
    def get_vacancies_filter(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """Абстрактный метод сортировки вакансий"""
        pass


class HH(GetApi):
    """Класс HH выполняет запрос к Api HH.ru и получает вакансии."""

    def __init__(self) -> None:
        """Инициализация класса HH"""
        self.url = "https://api.hh.ru/vacancies"

    def get_response(self, text: str, per_page: int) -> Response:
        """Запрос на Api HH.ru"""
        params: Any = {"text": f"Name:{text}", "per_page": per_page}
        response = requests.get(self.url, params=params)
        return response

    def get_vacancies_response(self, text: str, per_page: int) -> Any:
        """Получение вакансий с HH.ru"""
        vacancies = self.get_response(text, per_page).json()["items"]
        return vacancies

    def get_vacancies_filter(self, text: str, per_page: int = 15) -> Any:
        """Фильтрация вакансий"""
        filtered_vacancies = []
        vacancies_filtered = self.get_vacancies_response(text, per_page)
        for vacancy in vacancies_filtered:
            filtered_vacancies.append(
                {
                    "name": vacancy["name"],
                    "salary_from": vacancy["salary"]["from"],
                    "salary_to": vacancy["salary"]["to"],
                    "url": vacancy["alternate_url"],
                    "employer": vacancy["employer"]["name"],
                    "requirement": vacancy["snippet"]["requirement"],
                    "responsibility": vacancy["snippet"]["responsibility"],
                }
            )
        return filtered_vacancies
