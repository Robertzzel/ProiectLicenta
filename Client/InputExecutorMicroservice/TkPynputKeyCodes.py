from __future__ import annotations
from pynput.keyboard import Key
from typing import Optional


class KeyTranslator:
    MAPPED_KEYS = {
        16777216: Key.esc,
        16777217: Key.tab,
        16777219: Key.backspace,
        16777220: Key.enter,
        16777221: Key.enter,
        16777222: Key.insert,
        16777223: Key.delete,
        16777224: Key.pause,
        16777225: Key.print_screen,
        16777232: Key.home,
        16777233: Key.end,
        16777234: Key.left,
        16777235: Key.up,
        16777236: Key.right,
        16777237: Key.down,
        16777238: Key.page_up,
        16777239: Key.page_down,
        16777248: Key.shift,
        16777249: Key.ctrl,
        16777251: Key.alt,
        16777252: Key.caps_lock,
        16777253: Key.num_lock,
        16777254: Key.scroll_lock,
        16777264: Key.f1,
        16777265: Key.f2,
        16777266: Key.f3,
        16777267: Key.f4,
        16777268: Key.f5,
        16777269: Key.f6,
        16777270: Key.f7,
        16777271: Key.f8,
        16777272: Key.f9,
        16777273: Key.f10,
        16777274: Key.f11,
        16777275: Key.f12,
        16777276: Key.f13,
        16777277: Key.f14,
        16777278: Key.f15,
        16777279: Key.f16,
        16777280: Key.f17,
        16777281: Key.f18,
        16777282: Key.f19,
        16777283: Key.f20,
        16777299: Key.cmd,
        16777300: Key.cmd_r,
        16777301: Key.menu,
        32: Key.space,
    }

    @staticmethod
    def translate(keycode: int) -> Optional[Key | str]:
        result = None
        if keycode:
            result = KeyTranslator.MAPPED_KEYS.get(keycode, None)

        if result is None:
            try:
                return chr(keycode)
            except:
                return None

        return result
