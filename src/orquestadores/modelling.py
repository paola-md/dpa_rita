'''
SIMPLE
PYTHONPATH='.' AWS_PROFILE=dpa luigi --module modelling  RunModelSimple --local-scheduler

PARA UN MODELO
PYTHONPATH='.' AWS_PROFILE=dpa luigi --module modelling  RunModel --local-scheduler  --bucname models-dpa --numIt 2 --numPCA 3  --model LR --obj 0-1.5

PARA TODOS LOS MODELOS
PYTHONPATH='.' AWS_PROFILE=dpa luigi --module modelling RunAllTargets --local-scheduler  --bucname models-dpa --numIt 1 --numPCA 2  --model LR
'''

from luigi import Event, Task, build # Utilidades para acciones tras un task exitoso o fallido
from luigi.contrib.postgres import CopyToTable,PostgresQuery
from src.models.train_model import run_model
from src.models.save_model import parse_filename
import os
import luigi
import luigi.contrib.s3

# TASKS
#from src.orquestadores.feature_engineering import GetFEData
from tasks.metadatos_semantic import Metadata_Semantic
# ===============================
CURRENT_DIR = os.getcwd()
# ===============================

class RunModelSimple(luigi.Task):
	bucname = "models-dpa"
	numIt = 1
	numPCA = 3
	obj = "0-1.5"
	model = "LR"

	def requires(self):
		return Metadata_Semantic()

	def output(self):
		objetivo = self.obj
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		output_path = parse_filename(objetivo, model_name, hyperparams)
		output_path = "s3://" + str(self.bucname) +  output_path[1:] + ".model.zip"

		return luigi.contrib.s3.S3Target(path=output_path)

	def run(self):
		objetivo = self.obj
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		run_model(objetivo, model_name, hyperparams, True)


class RunModel(luigi.Task):
	bucname = luigi.Parameter()
	numIt = luigi.Parameter()
	numPCA = luigi.Parameter()
	obj = luigi.Parameter()
	model = luigi.Parameter()

	def requires(self):
		return Metadata_Semantic()

	def output(self):
		objetivo = self.obj
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		output_path = parse_filename(objetivo, model_name, hyperparams)
		output_path = "s3://" + str(self.bucname) +  output_path[1:] + ".model.zip"

		return luigi.contrib.s3.S3Target(path=output_path)

	def run(self):
		objetivo = self.obj
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		run_model(objetivo, model_name, hyperparams, True)
# =======================================================
# ALL TARGETS
# 1) "0-1.5"
# 2) "1.5-3.5"
# 3) "3.5-"
# 4) "cancelled"
# =======================================================
TARGET_A = "0-1.5"
TARGET_B = "1.5-3.5"
TARGET_C =  "3.5-"
TARGET_D = "cancelled"

class RunAllTargets(luigi.Task):
	bucname = luigi.Parameter()
	numIt = luigi.Parameter()
	numPCA = luigi.Parameter()
	model = luigi.Parameter()

	def requires(self):
		return  \
		 RunTargetA(self.bucname, self.numIt, self.numPCA, self.model), \
		 RunTargetB(self.bucname,self.numIt, self.numPCA, self.model), \
		 RunTargetC(self.bucname,self.numIt, self.numPCA, self.model), \
		 RunTargetD(self.bucname,self.numIt, self.numPCA, self.model)


class RunTargetA(luigi.Task):
	bucname = luigi.Parameter()
	numIt = luigi.Parameter()
	numPCA = luigi.Parameter()
	model = luigi.Parameter()

	def requires(self):
		return Metadata_Semantic()

	def output(self):
		objetivo = TARGET_A
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		output_path = parse_filename(objetivo, model_name, hyperparams)
		output_path = "s3://" + str(self.bucname) +  output_path[1:] + ".model.zip"

		return luigi.contrib.s3.S3Target(path=output_path)

	def run(self):
		objetivo = TARGET_A
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		run_model(objetivo, model_name, hyperparams, True)

class RunTargetB(luigi.Task):
	bucname = luigi.Parameter()
	numIt = luigi.Parameter()
	numPCA = luigi.Parameter()
	model = luigi.Parameter()

	def requires(self):
		return Metadata_Semantic()

	def output(self):
		objetivo = TARGET_B
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		output_path = parse_filename(objetivo, model_name, hyperparams)
		output_path = "s3://" + str(self.bucname) +  output_path[1:] + ".model.zip"

		return luigi.contrib.s3.S3Target(path=output_path)

	def run(self):
		objetivo = TARGET_B
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		run_model(objetivo, model_name, hyperparams, True)

class RunTargetC(luigi.Task):
	bucname = luigi.Parameter()
	numIt = luigi.Parameter()
	numPCA = luigi.Parameter()
	model = luigi.Parameter()

	def requires(self):
		return Metadata_Semantic()

	def output(self):
		objetivo = TARGET_C
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		output_path = parse_filename(objetivo, model_name, hyperparams)
		output_path = "s3://" + str(self.bucname) +  output_path[1:] + ".model.zip"

		return luigi.contrib.s3.S3Target(path=output_path)

	def run(self):
		objetivo = TARGET_C
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		run_model(objetivo, model_name, hyperparams, True)


class RunTargetD(luigi.Task):
	bucname = luigi.Parameter()
	numIt = luigi.Parameter()
	numPCA = luigi.Parameter()
	model = luigi.Parameter()

	def requires(self):
		return GetFEData()

	def output(self):
		objetivo = TARGET_D
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		output_path = parse_filename(objetivo, model_name, hyperparams)
		output_path = "s3://" + str(self.bucname) +  output_path[1:] + ".model.zip"

		return luigi.contrib.s3.S3Target(path=output_path)

	def run(self):
		objetivo = TARGET_D
		model_name = self.model
		hyperparams = {"iter": int(self.numIt),
						"pca": int(self.numPCA)}

		run_model(objetivo, model_name, hyperparams, True)
