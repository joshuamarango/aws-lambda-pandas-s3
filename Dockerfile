FROM public.ecr.aws/lambda/python:3.8

COPY . ./
CMD ["app.lambda_handler"]