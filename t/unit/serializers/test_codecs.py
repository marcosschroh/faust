import base64
from typing import Mapping
from faust.serializers.codecs import (
    Codec, binary as _binary, codecs, dumps, get_codec, json, loads, register,
)
from faust.utils import json as _json
from hypothesis import given
from hypothesis.strategies import binary, dictionaries, text
from mode.utils.compat import want_str
import pytest

DATA = {'a': 1, 'b': 'string'}


@pytest.mark.asyncio
async def test_interface():
    s = Codec()
    with pytest.raises(NotImplementedError):
        await s._loads(b'foo')
    with pytest.raises(NotImplementedError):
        await s.dumps(10)
    assert s.__or__(1) is NotImplemented


@pytest.mark.asyncio
@pytest.mark.parametrize('codec', ['json', 'pickle', 'yaml'])
async def test_json_subset(codec: str) -> None:
    assert await loads(codec, await dumps(codec, DATA)) == DATA


@pytest.mark.asyncio
@given(binary())
async def test_binary(input: bytes) -> None:
    assert await loads('binary', await dumps('binary', input)) == input


@pytest.mark.asyncio
@given(dictionaries(text(), text()))
async def test_combinators(input: Mapping[str, str]) -> None:
    s = json() | _binary()
    assert repr(s).replace("u'", "'") == 'json() | binary()'

    d = await s.dumps(input)
    assert isinstance(d, bytes)
    assert _json.loads(want_str(base64.b64decode(d))) == input


def test_get_codec():
    assert get_codec('json|binary')
    assert get_codec(Codec) is Codec


def test_register():
    try:
        class MyCodec(Codec):
            ...
        register('mine', MyCodec)
        assert get_codec('mine') is MyCodec
    finally:
        codecs.pop('mine')


@pytest.mark.asyncio
async def test_raw():
    bits = await get_codec('raw').dumps('foo')
    assert isinstance(bits, bytes)
    assert await get_codec('raw').loads(bits) == b'foo'
