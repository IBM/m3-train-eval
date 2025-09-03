import os
import re
from openai import OpenAI
from typing import List, Any
import urllib3
import spacy
from transformers import AutoTokenizer

import subprocess
import sys

nlp=None
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading spacy model...")
    subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
    import spacy
    nlp = spacy.load("en_core_web_sm")

product_counts = {}
urllib3.disable_warnings()


SCORER_PROMPT=""""The following tasks each contains a document, a conversation and a response to the last turn of the conversation. The response is supposed to rely on the document for its source of information, optionally using common sense knowledge and common sense inference, but it may fail this, and instead contain substantial claims that are not grounded in the document or common sense knowledge.

Your task is to assess whether the response is entirely grounded in the document, grounded in the document plus common sense knowledge and reasoning, or ungrounded. To make this determination, perform the following steps:
1. Identify all substantial claims in the response:
   - Ignore non-substantial claims, such as greetings or self-descriptions such as "I'm a helpful assistant",
   - Try to formulate each claim in a stand-alone form with all pronouns and other references resolved;
2. Assess the grounding of each of these claims:
   - If it is essentially a rephrasing of information from the document, or can be derived from such information by trivial common-sense reasoning, it is grounded,  This is so even if it contradicts other parts of the document.  
   - If it relies on, in additional to information from the document, additional non-trivial common sense knowledge or common sense reasoning, it is partially grounded,
   - If a claim is about the provided document, or about the agent's state of knowledge, with the effect of not being able to answer the user inquiry, it is grounded if and only if the required information is indeed lacking in the document.
   - If a claim cannot be derived directly from the document or indirectly with help of common sense knowledge and reasoning, it is ungrounded;
3. Make the overall decision according to:
   - If at least one claim is not grounded, the response is not grounded (Note that this is not a case of partially grounded);
   - Otherwise if at least one claim is partially grounded, the response is partially grounded;
   - Otherwise the response is grounded.

Your final conclusion should be written in two lines:
- The first line contains one of the following labels  [yes, partial, no, unsure],
  - "yes" is for grounded,
  - "partial" is for grounded with non-trivial common sense knowledge or reasoning,
  - "no" is for ungrounded,
  - "unsure" is for the situations where the document, conversation or response contain ambiguities such that different interpretations lead to different conclusions about groundedness;
- The second line is an explanation of your reasoning for the conclusion,
  - Following are some examples explanations for each label
    - The claim "..." in the response is not supported anywhere in the document.  Therefore the response is not grounded;
    - The claim "..." in the response relies on common sense knowledge "...". There are no ungrounded claims. Therefore the response is partially grounded;
    - The response contains claims A, B, ....  They are all grounded in document.  Therefore the response is grounded.
    - The response contains claim A, which may mean A1 and is grounded, or it may mean A2 and is not grounded.  Therefore the response groundedness is unsure.
  - Make explanation as concise as possible.  If the conclusion is "yes", briefly mention each substantial claims in the response and state that they are all grounded in the document.  Use phrases instead of full sentences to enumerate the claims to make this explanation concise.  If the conclusion is not "yes", the explanation should focus on the claim with the most severe violation.

Limit your answer and explanation to at most 200 words.


[Document]
The company offers the Dependent Care Spending Account program. It allows you to use pre-tax dollar for qualified dependent care expenses.


[Conversation]
Is there a benefit for childcare?


[Response]
Yes, the Dependent Care Spending Account program provides childcare benefits.


[Conclusion]
partial.
The claim "Dependent Care Spending Account program provides childcare benefits" relies on information from the document "Dependent Care Spending Account program provides dependent care  benefits" and the common sense knowledge that "childcare benefits is a typically included in  dependent care benefits".



[Document]
With this app you can create and maintain incoterms which are the official rules of the International Chamber of Commerce (ICC) regarding how to define trade terms.


Provide the Incoterms Version. An incoterms version is an edition containing a list of international terms for transportation that is defined by the International Chamber of Commerce (ICC).


[Conversation]
What are Incoterms?


[Response]
Incoterms are the official rules of the International Chamber of Commerce (ICC) regarding how to define trade terms.


[Conclusion]
yes. 
The response contains only a description "incoterms" which is a rephrasing of information from the document. 





[Document]
Flex Time on Election Day

Q: What is company's policy for employees who need to take time off to vote on Election Day?

A: Your job should never hinder you from exercising your right to vote. We have long supported flexible working on Election Day and will continue to do so this year. You should coordinate with your manager on flexibility needed for Election Day.

Q: Do I have to use PTO if I want to take time off to vote on Election Day?

A: No. We fully support giving you the flexibility they need to take time to vote on Election Day. This does not require using a vacation day or a personal choice holiday. Employees should coordinate with their managers.


[Conversation]
What is the policy for employees who need to take time off to vote on Election Day?


[Response]
As an inclusive company, the company fully supports employees' right to vote and encourages them to take time off to exercise their civic duty. The policy allows employees to take up to 2 hours of paid time off to vote on Election Day, as long as they provide reasonable advance notice to their manager. 


[Conclusion]
no. 
Even though the response contains several grounded claims, the claim "allowed to take up to 2 hours of paid time off to vote on Election Day" is directly contradicted by the document which implies up to a full day off allowed instead of merely 2 hours.



[Document]
The Moon is Earth's only natural satellite. It goes around the Earth at a distance of about 239,000 miles (385,000 kilometers). The Earth and Moon are tidally locked. Their rotations are so in sync we only see one side of the Moon.


[Conversation]
What is the period of ration of the moon?


[Response]
I do not have information to answer your question.


[Conclusion]
yes.
The response has a single claim of lack of information about the the period of ration of the moon, which is indeed lacking from the document.



[Document]
The Moon is Earth's only natural satellite. It goes around the Earth at a distance of about 239,000 miles (385,000 kilometers). The Earth and Moon are tidally locked. Their rotations are so in sync we only see one side of the Moon.


[Conversation]
Is the moon rotating around the the earth faster than it spins on its own axis?


[Response]
I do not have information to answer your question.


[Conclusion]
no.
The response has a single claim of lack of information comparing the moon's rate of rotation vs spin.  This lack of information is not true, as the document states that the rotation and spin are as fast as each other.



[Document]
Mercury is the smallest planet in our solar system and the nearest to the Sun. Mercury is only slightly larger than Earth's Moon. Its surface is covered in tens of thousands of impact craters.


[Conversation]
Which planet is the smallest in solar system?


[Response]
Mercury is the smallest planet in our solar system. It is only slightly larger than Earth's Moon. 


[Conclusion]
yes.
The response contains two claims, mercury being the smallest planet in the solar system and only slightly larger than the moon.  Both are directly supported by the document.



[Document]
{document}


[Conversation]
{inquiry}


[Response]
{response}


[Conclusion]
"""


def get_tokenizer(model_id="sentence-transformers/all-MiniLM-L6-v2"):
    model_path = model_id
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    return tokenizer

tokenizer=get_tokenizer()


def split_text(text: str, tokenizer, title: str = "", max_length: int = 256, stride: int = 50) \
        -> tuple[list[str], list[list[int | Any]]]:
    """
    Method to split a text into pieces that are of a specified <max_length> length, with the
    <stride> overlap, using a HF tokenizer.
    :param text: str - the text to split
    :param tokenizer: HF Tokenizer
       - the tokenizer to do the work of splitting the words into word pieces
    :param title: str - the title of the document
    :param max_length: int - the maximum length of the resulting sequence
    :param stride: int - the overlap between windows
    """
    #global nlp
    text = re.sub(r' {2,}', ' ', text, flags=re.MULTILINE)  # remove multiple spaces.
    if max_length is not None:
        # res = tokenizer(text, max_length=max_length, stride=stride,
        #                 return_overflowing_tokens=True, truncation=True)
        tok_len = get_tokenized_length(tokenizer, text)
        if tok_len <= max_length:
            return [text], [[0, len(text)]]
        else:
            if title:  # make space for the title in each split text.
                ltitle = get_tokenized_length(tokenizer, title)
                max_length -= ltitle
                ind = text.find(title)
                if ind == 0:
                    text = text[ind + len(title):]

            parsed_text = nlp(text)

            tsizes = []
            begins = []
            ends = []
            for sent in parsed_text.sents:
                stext = sent.text
                slen = get_tokenized_length(tokenizer, stext)
                if slen > max_length:
                    too_long = [[t for t in sent]]
                    too_long[0].reverse()
                    while len(too_long) > 0:
                        tl = too_long.pop()
                        ll = get_tokenized_length(tokenizer, text[tl[-1].idx:(tl[0].idx+len(tl[0].text))])
                        if ll <= max_length:
                            tsizes.append(ll)
                            begins.append(tl[-1].idx)
                            ends.append(tl[0].idx+len(tl[0].text))
                        else:
                            if len(tl) > 1:  # Ignore really long words
                                mid = int(len(tl) / 2)
                                too_long.extend([tl[:mid], tl[mid:]])
                            else:
                                pass
                else:
                    tsizes.append(slen)
                    begins.append(sent.start_char)
                    ends.append(sent.start_char+len(sent.text))

            intervals = compute_intervals(tsizes, max_length, stride)

            positions = [[begins[p[0]], ends[p[1]]] for p in intervals]
            texts = [text[p[0]:p[1]] for p in positions]
            return texts, positions


def compute_intervals(tsizes: List[int], max_length: int, stride: int) -> List[List[int | Any]]:
    """
    Computes a list of breaking points that satisfy the constraints on the max_length and
    stride (really, it's more of overlap).
    :param tsizes: list[int] - the lenghts (in word pieces) for the document segments (most likely sentences).
    :param max_length: int - the maximum length (in word pieces) for the each resulting text segment.
    :param stride: int - the minimum overlap between consecutive segments.
    :return: list[[int, int]] a list of start and end indices in the tsizes array, inclusive.
    """
    i = 1
    sum = tsizes[0]
    prev = 0
    intervals = []
    num_iters = 0
    while i < len(tsizes):
        if sum + tsizes[i] > max_length:
            if len(intervals) > 0 and intervals[-1][0] == prev:
                raise RuntimeError("You have a problem with the splitting - it's cycling!: {intervals[-3:]}")
            if num_iters > 10000:
                print(f"Too many tried - probably something is wrong with the document.")
                return intervals
            num_iters += 1
            intervals.append([prev, i - 1])
            if i > 1 and tsizes[i - 1] + tsizes[i] <= max_length:
                j = i - 1
                overlap = 0
                max_length_tmp = max_length - tsizes[i]  # the overlap + current size is not more than max_length
                while j > 0:
                    overlap += tsizes[j]
                    if overlap < stride and overlap + tsizes[j - 1] <= max_length_tmp:
                        j -= 1
                    else:
                        break
                i = j
            prev = i
            sum = 0
        else:
            sum += tsizes[i]
            i += 1
    intervals.append([prev, len(tsizes) - 1])
    return intervals


def get_tokenized_length(tokenizer, text):
    """
    Returns the size of the <text> after being tokenized by <tokenizer>
    :param tokenizer: Tokenizer - the tokenizer to convert text to word pieces
    :param text: str - the input text
    :return the length (in word pieces) of the tokenized text.
    """
    if tokenizer is not None:
        toks = tokenizer(text)
        return len(toks['input_ids'])
    else:
        return -1

def get_llm(model_id=None):
    # NOTE: Using mixtral 8x22b as default now. Can change in the future
    try:
        rits_api_key = os.environ["RITS_API_KEY"]
    except BaseException:
        raise ValueError(
            "You need to set the env var RITS_API_KEY to use a model from RITS."
        )
    return OpenAI(
        api_key=rits_api_key,
        base_url="https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/mixtral-8x22b-instruct-v01/v1",
        default_headers={"RITS_API_KEY": rits_api_key},
    )

def score_chunks(chunks: List[str], query: str, answer:str)->List[int]:
    chunk_index=[]
    model_id="mistralai/mixtral-8x22B-instruct-v0.1"
    llm = get_llm()
    for index,chunk in enumerate(chunks):
        formatted_string = SCORER_PROMPT.format(
        document=chunk,
        inquiry=query,
        response=answer
    )
        messages = [
        {
            "role": "user",
            "content": formatted_string
        }
    ]
        try:
            response = llm.chat.completions.create(
                        model=model_id,
                        messages=messages,
                        max_tokens=1024,
                    )
        except BaseException as e:
                    raise e
        judgement= response.choices[0].message.content
        if judgement.strip().lower().startswith("yes"):
            chunk_index.append(index)
    return chunk_index