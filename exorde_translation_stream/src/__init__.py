from __future__ import annotations
import sys
import ctranslate2
import os
import sentencepiece as spm
from pathlib import Path
from typing import List, Union, AsyncGenerator, Generator

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import SpanKind

# Initialize the tracer provider
resource = Resource(attributes={
    "service.name": "translation"
})
trace.set_tracer_provider(TracerProvider(resource=resource))
# Configure the OTLP exporter
otlp_exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)
# Use SimpleSpanProcessor to stream spans immediately
span_processor = SimpleSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
# Use the tracer for application-specific tracing
tracer = trace.get_tracer("translation")


class Language:
    translations_from: list[ITranslation] = []
    translations_to: list[ITranslation] = []

    def __init__(self, code: str, name: str):
        self.code = code
        self.name = name
        self.translations_from = []
        self.translations_to = []

    def __str__(self):
        return self.name

    def get_translation(self, to: Language) -> ITranslation | None:
        valid_translations = list(
            filter(lambda x: x.to_lang.code == to.code, self.translations_from)
        )
        if len(valid_translations) > 0:
            return valid_translations[0]
        return None

class ITranslation:
    from_lang: Language
    to_lang: Language

    def translate(self, input_text: str) -> str:
        return self.hypotheses(input_text, num_hypotheses=1)[0].value

    def hypotheses(self, input_text: str, num_hypotheses: int = 4) -> list[Hypothesis]:
        raise NotImplementedError()

    @staticmethod
    def split_into_paragraphs(input_text: str) -> list[str]:
        return input_text.split("\n")

    @staticmethod
    def combine_paragraphs(paragraphs: list[str]) -> str:
        return "\n".join(paragraphs)

    def __repr__(self):
        return str(self.from_lang) + " -> " + str(self.to_lang)

    def __str__(self):
        return repr(self).replace("->", "→")

class ITag:
    translateable: bool

    def text(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f'{str(type(self))} "{str(self.children)}"'

class Tag(ITag):
    def __init__(self, children: ITag | str, translateable: bool = True):
        self.children = children
        self.translateable = translateable

    def text(self) -> str:
        return "".join(
            [(child.text() if type(child) != str else child) for child in self.children]
        )

def depth(tag: ITag | str) -> int:
    if type(tag) is str:
        return 0
    if len(tag.children) == 0:
        return 0
    return max([depth(t) for t in tag.children])

async def translate_preserve_formatting(
    underlying_translation: ITranslation, input_text: str
) -> AsyncGenerator[str, None]:
    async for translated_text in underlying_translation.translate(input_text):
        if len(input_text) > 0:
            if input_text[0] == " " and not (
                len(translated_text) > 0 and translated_text[0] == " "
            ):
                translated_text = " " + translated_text
            if input_text[-1] == " " and not (
                len(translated_text) > 0 and translated_text[-1] == " "
            ):
                translated_text = translated_text + " "
        yield translated_text

async def inject_tags_inference(
    underlying_translation: ITranslation, tag: ITag
) -> ITag | None:
    MAX_SEQUENCE_LENGTH = 200

    text = tag.text()
    if len(text) > MAX_SEQUENCE_LENGTH:
        return None

    translated_text = ""
    async for part in translate_preserve_formatting(underlying_translation, text):
        translated_text += part

    class InjectionTag:
        def __init__(self, text: str, tag: ITag):
            self.text = text
            self.tag = tag
            self.injection_index = None

    injection_tags = []
    for child in tag.children:
        if depth(child) == 1:
            translated = ""
            async for part in translate_preserve_formatting(
                underlying_translation, child.text()
            ):
                translated += part
            injection_tags.append(InjectionTag(translated, child))
        elif type(child) is not str:
            return None

    for injection_tag in injection_tags:
        injection_index = translated_text.find(injection_tag.text)
        if injection_index != -1:
            injection_tag.injection_index = injection_index
        else:
            return None

    injection_tags.sort(key=lambda x: x.injection_index)
    for i in range(len(injection_tags) - 1):
        injection_tag = injection_tags[i]
        next_injection_tag = injection_tags[i + 1]
        if (
            injection_tag.injection_index + len(injection_tag.text)
            >= next_injection_tag.injection_index
        ):
            return None

    to_return = []
    i = 0
    for injection_tag in injection_tags:
        if i < injection_tag.injection_index:
            to_return.append(translated_text[i : injection_tag.injection_index])
        to_return.append(injection_tag.tag)
        i = injection_tag.injection_index + len(injection_tag.text)
    if i < len(translated_text):
        to_return.append(translated_text[i:])

    tag.children = to_return

    return tag

async def translate_tags(
    underlying_translation: ITranslation, tag: ITag | str
) -> AsyncGenerator[ITag | str, None]:
    if isinstance(tag, str):
        async for r in translate_preserve_formatting(underlying_translation, tag):
            yield r
        return

    if tag.translateable is False:
        yield tag
        return

    if depth(tag) == 2:
        tag_injection = await inject_tags_inference(underlying_translation, tag)
        if tag_injection is not None:
            yield tag_injection
            return

    translated_children = []
    for child in tag.children:
        async for translated_child in translate_tags(underlying_translation, child):
            translated_children.append(translated_child)

    tag.children = translated_children
    yield tag

class Tokenizer:
    def encode(self, sentence: str) -> List[str]:
        raise NotImplementedError()
    
    def decode(self, tokens: List[str]) -> str:
        raise NotImplementedError()

class SentencePieceTokenizer(Tokenizer):
    def __init__(self, model_file: Path):
        self.model_file = model_file
        self.processor = None

    def lazy_processor(self) -> spm.SentencePieceProcessor:
        if self.processor is None:
            self.processor = spm.SentencePieceProcessor()
            self.processor.Load(str(self.model_file))
        return self.processor

    def encode(self, sentence: str) -> List[str]:
        tokens = self.lazy_processor().encode(sentence, out_type=str)
        return tokens

    def decode(self, tokens: List[str]) -> str:
        return self.lazy_processor().decode_pieces(tokens)

def translated_text_to_string(translated_result):
    words = []
    current_word = ''
    for token in translated_result.hypotheses[0]:
        if token.startswith('▁'):
            if current_word:
                words.append(current_word)
            current_word = token[1:]
        else:
            current_word += token
    if current_word:
        words.append(current_word)
    return ' '.join(words)

def detect_and_tag_text(input_file: str) -> Generator[Tag, None, None]:
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            if "non-translatable" in line:
                yield Tag([line.strip()], translateable=False)
            else:
                yield Tag([line.strip()], translateable=True)

def detect_and_tag_from_input() -> Generator[Tag, None, None]:
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if "non-translatable" in line:
            yield Tag([line.strip()], translateable=False)
        else:
            yield Tag([line.strip()], translateable=True)

class TranslationWrapper:
    def __init__(self, translator, tokenizer):
        self.translator = translator
        self.tokenizer = tokenizer

    async def translate(self, input_text: str) -> AsyncGenerator[str, None]:
        tokenized_input = self.tokenizer.encode(input_text)
        for translation_results in self.translator.translate_iterable([tokenized_input]):
            translated_sentence = translated_text_to_string(translation_results)
            yield translated_sentence

MODEL_MAP = {
    "fr": "translate-fr_en-1_9",
    "pl": "translate-pl_en-1_9",
    "az": "translate-az_en-1_5",
    "bg": "translate-bg_en-1_9",
    "bn": "translate-bn_en-1_9",
    "cs": "translate-cs_en-1_9",
    "da": "translate-da_en-1_3",
    "el": "translate-el_en-1_9",
    "et": "translate-et_en-1_9",
    "fa": "translate-fa_en-1_5",
    "fr": "translate-fr_en-1_9",
    "he": "translate-he_en-1_5",
    "lt": "translate-lt_en-1_9",
    "lv": "translate-lv_en-1_9",
    "ms": "translate-ms_en-1_9",
    "nb": "translate-nb_en-1_9",
    "nl": "translate-nl_en-1_8",
    "ro": "translate-ro_en-1_9",
    "ru": "translate-ru_en-1_9",
    "sk": "translate-sk_en-1_5",
    "sl": "translate-sl_en-1_9",
    "sq": "translate-sq_en-1_9",
    "th": "translate-th_en-1_9",
    "tl": "translate-tl_en-1_9",
    "tr": "translate-tr_en-1_5",
    "zt": "translate-zt_en-1_9",
    "zh": "translate-zh_en-1_9",
    "ar": "ar_en",
    "de": "de_en",
    "es": "es_en",
    "fi": "fi_en",
    "ga": "ga_en",
    "hi": "hi_en",
    "hu": "hu_en",
    "id": "id_en",
    "it": "it_en",
    "ja": "ja_en",
    "ko": "ko_en",
    "pt": "pt_en",
    "sv": "sv_en",
}

async def main(args):
    with tracer.start_as_current_span("translation_subprocess", kind=SpanKind.SERVER) as main_span:
        # Span for verifying model existence
        with tracer.start_as_current_span("model-verification") as verify_span:
            with tracer.start_as_current_span("check-model-map") as check_model_span:
                if args not in MODEL_MAP:
                    raise Exception("Language '{}' has no existing model".format(args))
                check_model_span.set_attribute("model_exists", True)
                verify_span.add_event("Model verification complete")

        # Span for model and tokenizer loading
        with tracer.start_as_current_span("model-and-tokenizer-loading") as load_span:
            # Subspan for determining paths
            with tracer.start_as_current_span("set-paths") as paths_span:
                home_directory = os.getenv("HOME")
                model_path = f"{home_directory}/.config/exorde/assets/{MODEL_MAP[args]}/model"
                tokenizer_path = Path(f"{home_directory}/.config/exorde/assets/{MODEL_MAP[args]}/sentencepiece.model")
                paths_span.set_attribute("model_path", model_path)
                paths_span.set_attribute("tokenizer_path", str(tokenizer_path))
                paths_span.add_event("Paths set for model and tokenizer")

            # Subspan for initializing tokenizer
            with tracer.start_as_current_span("initialize-tokenizer") as tokenizer_span:
                tokenizer = SentencePieceTokenizer(tokenizer_path)
                tokenizer_span.add_event("Tokenizer initialized")

            # Subspan for initializing translator
            with tracer.start_as_current_span("initialize-translator") as translator_span:
                translator = ctranslate2.Translator(model_path)
                translator_span.add_event("Translator initialized")

            load_span.add_event("Model and tokenizer loading complete")

        # Span for processing translation
        with tracer.start_as_current_span("translation-processing") as translation_span:
            translation_wrapper = TranslationWrapper(translator, tokenizer)
            
            for tagged_text in detect_and_tag_from_input():
                async for translated_tagged_text in translate_tags(translation_wrapper, tagged_text):
                    print(translated_tagged_text.text() if isinstance(translated_tagged_text, Tag) else translated_tagged_text)

            translation_span.add_event("All tagged text processed")



def run(args):
    import asyncio
    asyncio.run(main(args))

if __name__ == "__main__":
    import asyncio
    asyncio.run(main(sys.argv[1]))
