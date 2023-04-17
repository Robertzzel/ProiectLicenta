from __future__ import annotations
from pynput.keyboard import Key
from typing import Optional


class KeyTranslator:
    MAPPED_KEYS = {
        # 65513: Key.alt,
        65513: Key.alt_l,
        65514: Key.alt_r,
        # Key.alt_gr
        65288: Key.backspace,
        65509: Key.caps_lock,
        # Key.cmd
        # Key.cmd_l
        # Key.cmd_r
        # Key.ctrl
        65507: Key.ctrl_l,
        65508: Key.ctrl_r,
        65535: Key.delete,
        65364: Key.down,
        65367: Key.end,
        65293: Key.enter,
        65307: Key.esc,
        65470: Key.f1,
        65471: Key.f2,
        65472: Key.f3,
        65473: Key.f4,
        65474: Key.f5,
        65475: Key.f6,
        65476: Key.f7,
        65477: Key.f8,
        65478: Key.f9,
        65479: Key.f10,
        # 95: Key.f11
        # 96: Key.f12
        # Key.f13
        # Key.f14
        # Key.f15
        # Key.f16
        # Key.f17
        # Key.f18
        # Key.f19
        # Key.f20
        65360: Key.home,
        65361: Key.left,
        65366: Key.page_down,
        65365: Key.page_up,
        65363: Key.right,
        # Key.shift
        65505: Key.shift_l,
        65506: Key.shift_r,
        32: Key.space,
        65289: Key.tab,
        65362: Key.up,
        # Key.media_play_pause
        # Key.media_volume_mute
        #Key.media_volume_down
        # Key.media_volume_up
        # Key.media_previous
        # Key.media_next
        65379: Key.insert,
        65383: Key.menu,
        65407: Key.num_lock,
        65299: Key.pause,
        65377: Key.print_screen,
        65300: Key.scroll_lock,
    }

    @staticmethod
    def translate(keycode: int) -> Optional[Key | str]:
        result = None
        if keycode:
            result = KeyTranslator.MAPPED_KEYS.get(keycode, None)

        if result is None:
            return chr(keycode)

        return result
