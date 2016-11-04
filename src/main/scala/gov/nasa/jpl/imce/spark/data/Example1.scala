package gov.nasa.jpl.imce.spark.data

import java.lang.System

import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import scala.util.control.Exception._
import java.io.InputStream

import gov.nasa.jpl.imce.omf.schema._
import gov.nasa.jpl.imce.omf.schema.tables.{IRI,LocalName,UUID}
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipFile}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.immutable._
import scala.{Array,StringContext,Unit}
import scala.Predef.{refArrayOps,String}

import scala.io

object Example1 {

  case class TerminologyGraphData( uuid: UUID, kind: String, name: LocalName, iri: IRI)

  object TerminologyGraphData {

    implicit def toTerminologyGraphData
    (tg: tables.TerminologyGraph)
    : TerminologyGraphData
    = TerminologyGraphData(tg.uuid, tg.kind.toString, tg.name, tg.iri)

    implicit def toTerminologyGraph
    (tgd: TerminologyGraphData)
    : tables.TerminologyGraph
    = tables.TerminologyGraph(tgd.uuid, tables.TerminologyGraphKind.fromString(tgd.kind), tgd.name, tgd.iri)

  }

  def readJSonTable[T](is: InputStream, fromJSon: String => T)
  : Seq[T]
  = io.Source.fromInputStream(is).getLines.map(fromJSon).to[Seq]

  case class OMFTables
  (graphs: Seq[tables.TerminologyGraph] = Seq.empty,
   concepts: Seq[tables.Concept] = Seq.empty,
   conceptSpecializations: Seq[tables.ConceptSpecializationAxiom] = Seq.empty,
   binaryScalarRestrictions: Seq[tables.BinaryScalarRestrictionAxiom] = Seq.empty) {

    def readGraphs(is: InputStream)
    : OMFTables
    = copy(graphs = readJSonTable(is, tables.TerminologyGraphHelper.fromJSON))

    def readConcepts(is: InputStream)
    : OMFTables
    = copy(concepts = readJSonTable(is, tables.ConceptHelper.fromJSON))

    def readConceptSpecializations(is: InputStream)
    : OMFTables
    = copy(conceptSpecializations = readJSonTable(is, tables.ConceptSpecializationAxiomHelper.fromJSON))

    def readBinaryScalarRestrictions(is: InputStream)
    : OMFTables
    = copy(binaryScalarRestrictions = readJSonTable(is, tables.BinaryScalarRestrictionAxiomHelper.fromJSON))

    override def toString: String =
    "OMFTables{"+
    graphs.map(tables.TerminologyGraphHelper.toJSON).mkString("\ngraphs:{\n",",\n","\n}")+
    concepts.map(tables.ConceptHelper.toJSON).mkString("\nconcepts:{\n",",\n","\n}")+
    conceptSpecializations.map(tables.ConceptSpecializationAxiomHelper.toJSON).mkString("\nconceptSpecializations:{\n",",\n","\n}")+
    "}"

    def toDS
    ()
    (implicit spark: SparkSession)
    : OMFDataSet
    = {
      import spark.implicits._
      OMFDataSet(
        graphs=graphs.map(TerminologyGraphData.toTerminologyGraphData).toDS(),
        concepts=concepts.toDS(),
        conceptSpecializations=conceptSpecializations.toDS(),
        binaryScalarRestrictions=binaryScalarRestrictions.toDS())
    }
  }

  def mergeTables
  (t1: OMFTables, t2: OMFTables)
  : OMFTables
  = OMFTables(
    graphs = t1.graphs ++ t2.graphs,
    concepts = t1.concepts ++ t2.concepts,
    conceptSpecializations = t1.conceptSpecializations ++ t2.conceptSpecializations,
    binaryScalarRestrictions = t1.binaryScalarRestrictions ++ t2.binaryScalarRestrictions)

  def readZipArchive
  (zipFile: ZipFile)
  (tables: OMFTables, ze: ZipArchiveEntry)
  : OMFTables
  = {
    val is = zipFile.getInputStream(ze)
    ze.getName match {
      case "TerminologyGraphs.json" =>
        tables.readGraphs(is)
      case "Concepts.json" =>
        tables.readConcepts(is)
      case "ConceptSpecializationAxioms.json" =>
        tables.readConceptSpecializations(is)
    }
  }

  case class OMFDataSet
  (graphs: Dataset[TerminologyGraphData],
   concepts: Dataset[tables.Concept],
   conceptSpecializations: Dataset[tables.ConceptSpecializationAxiom],
   binaryScalarRestrictions: Dataset[tables.BinaryScalarRestrictionAxiom])

  /**
    * @param args The 1st argument must be an absolute path to a ZIP file of OMF Schema tables.
    */
  def main(args: Array[String]): Unit = {

    System.out.println(s"args: ${args.length}")
    args.foreach { arg =>
      System.out.println(s"Arg: $arg")
    }
    if (args.length != 2) {
      System.err.println(s"Usage: ${args(0)} <OMF Schema table ZIP file>")
      System.exit(-1)
    }

    run(args(1))
  }

  def run(omfSchemaJsonZipFile: String): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    implicit val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    try {
      nonFatalCatch[Unit]
        .withApply {
          (cause: java.lang.Throwable) =>
            System.out.println(s"Error: $cause")
            cause.printStackTrace()
        }
        .apply {
          val zipFile = new ZipFile(omfSchemaJsonZipFile)
          val tables =
            zipFile
              .getEntries
              .toIterable
              .par
              .aggregate(OMFTables())(seqop = readZipArchive(zipFile), combop = mergeTables)
          zipFile.close()

          System.out.println(s"tables: ")
          System.out.println(tables)

          val dtables = tables.toDS()
          System.out.println(s"\n as DS: ")
          System.out.println(dtables.concepts)

        }
    } finally {
      spark.stop()
    }
  }

}
