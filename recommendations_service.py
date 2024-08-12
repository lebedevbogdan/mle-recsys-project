import logging as logger
import pandas as pd

from fastapi import FastAPI
from contextlib import asynccontextmanager

class Recommendations:

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path, **kwargs):
        """
        Загружает рекомендации из файла
        """

        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path, **kwargs)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logger.info(f"Loaded")

    def get(self, user_id: int, k: int=100):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
            print('check_1')
        except KeyError:
            print('check_2')
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except:
            print('check_3')
            logger.error("No recommendations found")
            recs = []

        return recs

    def stats(self):

        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")

class SimilarItems:

    def __init__(self):

        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Loading data, type: {type}")
        self._similar_items = pd.read_parquet(path, **kwargs).set_index('track_id')
        logger.info(f"Loaded")

    def get(self, item_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["track_id_recommended", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("No recommendations found")
            i2i = {"track_id_recommended": [], "score": {}}

        return i2i
    
class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = None
        self.max_events_per_user = max_events_per_user

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Loading data, type: {type}")
        self.events = pd.read_parquet(path, **kwargs).set_index('user_id')
        logger.info(f"Loaded")
    
    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        try:
            user_events = self.events.loc[user_id].sort_values(by='track_seq', ascending=False).head(k)['track_id'].to_list()
        except:
            logger.error("No recommendations found")
            user_events = []
        return user_events

events_store = EventStore()

sim_items_store = SimilarItems()

rec_store = Recommendations()

events_store.load(
    "events_train_sample.parquet",
    columns=["user_id", "track_id", "track_seq"],
)

sim_items_store.load(
    "similar_items_sample.parquet",
    columns=["track_id", "track_id_recommended", "score"],
)

rec_store.load(
    "personal",
    "als_recommendations_sample.parquet",
    columns=["user_id", "track_id", "score"],
    )
rec_store.load(
    "default",
    "top_popular.parquet",
    columns=["track_id", "score"],
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

async def recommendations_offline(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)

    return {"recs": recs}

async def recommendations_online(user_id: int, k: int = 1):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    user_events = events_store.get(user_id, k)

    # получаем список похожих объектов
    if len(user_events) > 0:
        item_id = user_events[0]
        recs = sim_items_store.get(item_id)['track_id_recommended']
    else:
        recs = []

    
    return {"recs": recs}


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []
    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        if i % 2==0:
            recs_blended.append(recs_offline[i])
        else:
            recs_blended.append(recs_online[i])
    if len(recs_offline) < len(recs_online):
        recs_blended += recs_online[min_length:]
    else:
        recs_blended += recs_offline[min_length:]

    return {"recs": recs_blended}
