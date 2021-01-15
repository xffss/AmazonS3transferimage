import os

import logging
import boto3
import json
from PIL import Image, ExifTags, ImageOps, ImageDraw,ImageFont
import io
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('s3')


# auto orientation base on exif info from image
def auto_exif_orientation(image):
    try:
        exif = image._getexif()
    except AttributeError:
        exif = None
    if exif is None:
        logger.info('no exif_orientation')
        return image
    exif = {ExifTags.TAGS[k]: v for k, v in exif.items() if k in ExifTags.TAGS}
    orientation = exif.get('Orientation', None)
    logger.info(f'original orientation:{orientation} to:')

    if orientation == 1:
        logger.info('no change')
        return image
    elif orientation == 2:
        logger.info('left-to-right mirror')
        return ImageOps.mirror(image)
    elif orientation == 3:
        logger.info('rotate 180')
        return image.transpose(Image.ROTATE_180)
    elif orientation == 4:
        logger.info('top-to-bottom mirror')
        return ImageOps.flip(image)
    elif orientation == 5:
        logger.info('top-to-left mirror')
        return ImageOps.mirror(image.transpose(Image.ROTATE_270))
    elif orientation == 6:
        logger.info('rotate 270')
        return image.transpose(Image.ROTATE_270)
    elif orientation == 7:
        logger.info('top-to-right mirror')
        return ImageOps.mirror(image.transpose(Image.ROTATE_90))
    elif orientation == 8:
        logger.info('rotate 90')
        return image.transpose(Image.ROTATE_90)
    else:
        return image

# get img from s3
def load_s3(bucket, key):
    logger.info(f'load from s3://{bucket}/{key}')

    try:
        response = client.get_object(
            Bucket=bucket,
            Key=key
        )
        logger.info(json.dumps(response, default=str))
        body = response['Body'].read()
    except Exception as e:
        logging.error(json.dumps(e, default=str))
        os._exit(0)
    return body

# watermake

def watermake(body,label):
    logger.info('Watermaking ...')
    
    try:
        im = Image.open(io.BytesIO(body))
        im = auto_exif_orientation(im)
        draw = ImageDraw.Draw(im)
        print(im.size)
        w, h = im.size
        margin = int(h/20)
        
        
        ft = ImageFont.truetype("/opt/Poppins-SemiBold.ttf", int(h/20))

        textWidth, textHeight = draw.textsize(label,font = ft)

        x = (w - textWidth) / 2
        y = h - textHeight - margin
        draw.text((x, y), label,font = ft, fill = 'gray' )
        in_mem_img = io.BytesIO()
        
        im.save(in_mem_img, format='jpeg',quality=95, progressive=True)      
        
        in_mem_img.seek(0)
        
    except Exception as e:
        logging.error(json.dumps(e, default=str))
        os._exit(0)
    return in_mem_img

def resizeimg(body,size):
    logger.info('Resizing ...')
    try:
        im = Image.open(io.BytesIO(body))
        im = auto_exif_orientation(im)
        print(im.size)
        w ,h = int(size[:size.find('x')]), int(size[size.find('x')+1:])
        im = im.resize((w, h), resample=Image.BICUBIC)
        in_mem_img = io.BytesIO()        
        im.save(in_mem_img, format='jpeg',quality=95, progressive=True)      

        in_mem_img.seek(0)

    except Exception as e:
        logging.error(json.dumps(e, default=str))
        os._exit(0)
    return in_mem_img

# put img to s3
def save_s3(bucket, key, body):
    logger.info(f'save to s3://{bucket}/{key}')
    try:
        response = client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body
        )
        logger.info(json.dumps(response, default=str))
    except Exception as e:
        logging.error(json.dumps(e, default=str))
        os._exit(0)
    return

# change prefix from input/ to output/ and change postfix
def change_key(key):
    key_pre = os.path.splitext(key)[0].replace('input', 'output', 1)
    key_new = key_pre+os.path.splitext(key)[1]

    return key_new


def lambda_handler(event, context):
    logger.info(json.dumps(event, default=str))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    img_org = load_s3(bucket, key)
    
    file_name = os.path.splitext(key)[1]
    

    if '_w_'in key:
        logger.info('Watermaking')
        label = key[key.find('_w_')+3:key.find('.')]
        logger.info('watermaking label : '+ label)
        body = watermake(img_org,label)

    elif '_s_' in key:
        logger.info('Resizing')
        size = key[key.find('_s_')+3:key.find('.')]
        logger.info('Resizing image to : '+ size)
        body = resizeimg(img_org,size)

    
    key_new = change_key(key )
    save_s3(bucket, key_new, body)

    return {
        'statusCode': 200,
        'body': 'OK'
    }
