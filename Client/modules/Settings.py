import platform
from pathlib import Path


class Settings:
    PLATFORM = platform.system().lower()
    MENU_WIDTH = 240
    RIGHT_BOX_WIDTH = 240
    TIME_ANIMATION = 500
    MENU_SELECTED_DARK_STYLESHEET = """
    border-left: 22px solid qlineargradient(spread:pad, x1:0.034, y1:0, x2:0.216, y2:0, stop:0.499 rgba(255, 121, 198, 255), stop:0.5 rgba(85, 170, 255, 0));
    background-color: rgb(40, 44, 52);
    """

    MENU_SELECTED_LIGHT_STYLESHEET = """
    border-left: 22px solid qlineargradient(spread:pad, x1:0.034, y1:0, x2:0.216, y2:0, stop:0.499 rgba(255, 121, 198, 255), stop:0.5 rgba(85, 170, 255, 0));
    background-color: #495474;
    """
    MENU_SELECTED_STYLESHEET = MENU_SELECTED_DARK_STYLESHEET
    TOP_LOGO_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'logo.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'logo.png'});".replace("\\", "/")
    TOGGLE_BUTTON_UL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-menu.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-menu.png'});".replace('\\', '/')
    BUTTON_LOGIN_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-user.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-user.png'});".replace('\\', '/')
    BUTTON_REGISTER_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-user-follow.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-user-follow.png'});".replace('\\', '/')
    BUTTON_CALL_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-laptop.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-laptop.png'});".replace('\\', '/')
    BUTTON_MY_VIDEOS_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-media-play.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-media-play.png'});".replace('\\', '/')
    BUTTON_CHANGE_THEME_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-star.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-star.png'});".replace('\\', '/')
    BUTTON_KAFKA_URL = f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-cloudy.png'});" if PLATFORM == "linux" else f"background-image: url({Path(__file__).parent.parent / 'images' / 'icons' / 'cil-cloudy.png'});".replace('\\', '/')