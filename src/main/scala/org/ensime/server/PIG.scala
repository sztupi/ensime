/**
  *  Copyright (c) 2010, Aemon Cannon
  *  All rights reserved.
  *
  *  Redistribution and use in source and binary forms, with or without
  *  modification, are permitted provided that the following conditions are met:
  *      * Redistributions of source code must retain the above copyright
  *        notice, this list of conditions and the following disclaimer.
  *      * Redistributions in binary form must reproduce the above copyright
  *        notice, this list of conditions and the following disclaimer in the
  *        documentation and/or other materials provided with the distribution.
  *      * Neither the name of ENSIME nor the
  *        names of its contributors may be used to endorse or promote products
  *        derived from this software without specific prior written permission.
  *
  *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  *  DISCLAIMED. IN NO EVENT SHALL Aemon Cannon BE LIABLE FOR ANY
  *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

package org.ensime.server

import java.io.File
import org.ensime.indexer.{IndexProducers, Tokens}
import org.ensime.model.{IndexSearchResult, SymbolSearchResult, TypeSearchResult}
import org.ensime.util.{FileUtils, Profiling, StringSimilarity, Util}
import org.neo4j.cypher.{ExecutionEngine, ExecutionResult}
import org.neo4j.graphdb.{DynamicRelationshipType, GraphDatabaseService, Node, Transaction}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.index.{Index, IndexHits, IndexManager}
import org.neo4j.helpers.collection.MapUtil
import scala.actors._
import scala.actors.Actor._
import scala.collection.{Iterable, JavaConversions}
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.HashMap


trait PIGIndex extends StringSimilarity with IndexProducers {

  private val cache = new HashMap[(String, String), Int]
  def editDist(a: String, b: String): Int = {
    cache.getOrElseUpdate((a, b), getLevenshteinDistance(a, b))
  }

  val PropName = "name"
  val PropNameTokens = "nameTokens"
  val PropQualNameTokens = "qualNameTokens"
  val PropPath = "path"
  val PropMd5 = "md5"
  val PropNodeType = "nodeType"
  val PropOffset = "offset"
  val PropDeclaredAs = "declaredAs"

  val NodeTypeFile = "file"
  val NodeTypePackage = "package"
  val NodeTypeType = "type"
  val NodeTypeMethod = "method"
  val NodeTypeImport = "import"
  val NodeTypeParam = "param"
  val NodeTypeVarDef = "varDef"
  val NodeTypeValDef = "valDef"
  val NodeTypeVarField = "varField"
  val NodeTypeValField = "valField"

  val RelMemberOf = DynamicRelationshipType.withName("memberOf")
  val RelInFile = DynamicRelationshipType.withName("inFile")
  val RelContainedBy = DynamicRelationshipType.withName("containedBy")
  val RelParamOf = DynamicRelationshipType.withName("paramOf")
  val RelFileOf = DynamicRelationshipType.withName("fileOf")

  trait RichNode {
    def node: Node
  }

  implicit def richNodeToNode(richNode: RichNode): Node = richNode.node

  trait AbstractSymbolNode extends RichNode {
    def name = node.getProperty(PropName, "").asInstanceOf[String]
    def localName = node.getProperty(PropName, "").asInstanceOf[String]
    def offset = node.getProperty(PropOffset, 0).asInstanceOf[Integer]
    def declaredAs =
      scala.Symbol(node.getProperty(PropDeclaredAs, "sym").asInstanceOf[String])
  }

  case class SymbolNode(node: Node) extends AbstractSymbolNode {}

  case class PackageNode(node: Node) extends AbstractSymbolNode {}

  case class FileNode(node: Node) extends RichNode {
    def path = node.getProperty(PropPath, "").asInstanceOf[String]
    def md5 = Option(node.getProperty(PropMd5, null).asInstanceOf[String])
  }

  case class DbInTransaction(
    db: GraphDatabaseService, transaction: Transaction) {}

  protected def graphDb: GraphDatabaseService
  protected def fileIndex: Index[Node]
  protected def tpeIndex: Index[Node]
  protected def scopeIndex: Index[Node]
  protected def scalaLibraryJar: File
  protected def projectRoot: File
  protected def filterPredicate(qualifiedName:String):Boolean = true

  protected def createDefaultGraphDb =
    (new GraphDatabaseFactory()).newEmbeddedDatabase(
      projectRoot.getPath + "/.ensime-neo4j-db")

  protected def createDefaultFileIndex(db: GraphDatabaseService) =
    db.index().forNodes("fileIndex")

  protected def createFullTextIndex(db: GraphDatabaseService, name: String) =
    db.index().forNodes(name,
      MapUtil.stringMap(
        IndexManager.PROVIDER, "lucene",
        "type", "fulltext",
        "analyzer", "org.apache.lucene.analysis.SimpleAnalyzer"
      ))

  protected def createDefaultTypeIndex(db: GraphDatabaseService) =
    createFullTextIndex(db, "tpeIndex")

  protected def createDefaultScopeIndex(db: GraphDatabaseService) =
    createFullTextIndex(db, "scopeIndex")

  protected def shutdown = { graphDb.shutdown }

  protected def doTx(f: DbInTransaction => Unit): Unit = {
    val tx = DbInTransaction(graphDb, graphDb.beginTx())
    try {
      f(tx)
      tx.transaction.success()
    } finally {
      tx.transaction.finish()
    }
  }

  protected def executeQuery(
    db: GraphDatabaseService, q: String): ExecutionResult = {
    val engine = new ExecutionEngine(graphDb)
    engine.execute(q)
  }

  // Find all types with names similar to typeName.
  def findTypeSuggestions(
    db: GraphDatabaseService,
    typeName:String,
    maxResults: Int,
    enclosingPackage: Option[String]
  ): Iterable[TypeSearchResult] = {
    val keys = Tokens.splitTypeName(typeName).
      filter(!_.isEmpty).map(_.toLowerCase)
    val luceneQuery = keys.map("nameTokens:" + _).mkString(" OR ")
    val result = executeQuery(
      db, s"""START n=node:tpeIndex('$luceneQuery')
              MATCH n-[:containedBy*1..5]->x, n-[:containedBy*1..5]->y
              WHERE x.nodeType='file' and y.nodeType='package'
              RETURN n,x,y LIMIT $maxResults""")
    val candidates = result.flatMap { row =>
      (row.get("n"), row.get("x"), row.get("y")) match {
        case (Some(tpeNode:Node), Some(fileNode:Node), Some(packNode:Node)) => {
          val tpe = SymbolNode(tpeNode)
          val file = FileNode(fileNode)
          val pack = PackageNode(packNode)
          val result = TypeSearchResult(
            pack.name + "." + tpe.name, tpe.localName, tpe.declaredAs,
            Some((file.path, tpe.offset)))
          enclosingPackage match {
            case Some(requiredPack) if requiredPack == pack.name => Some(result)
            case None => Some(result)
            case _ => None
          }
        }
        case _ => None
      }
    }.toIterable

    // Sort by edit distance of type name primarily, and
    // length of full name secondarily.
    candidates.toList.sortWith { (a, b) =>
      val d1 = editDist(a.localName, typeName)
      val d2 = editDist(b.localName, typeName)
      if (d1 == d2) a.name.length < b.name.length
      else d1 < d2
    }
  }

  // Find all occurances of a particular token.
  def findOccurences(db: GraphDatabaseService,
    name:String, maxResults: Int): Iterable[IndexSearchResult] = {
    val luceneQuery = "nameTokens:" + name.toLowerCase
    val result = executeQuery(db, s"""START n=node:scopeIndex('$luceneQuery')
                    MATCH n-[:containedBy*1..5]->x
                    WHERE x.nodeType='file'
                    RETURN n,x LIMIT $maxResults""")
    result.flatMap { row =>
      (row.get("n"), row.get("x")) match {
        case (Some(symNode:Node), Some(fileNode:Node)) => {
          val scope = SymbolNode(symNode)
          val file = FileNode(fileNode)
          Some(SymbolSearchResult(
            scope.name, scope.localName, scope.declaredAs,
            Some((file.path, scope.offset))))
        }
        case _ => None
      }
    }.toIterable
  }

  // Find all occurances of a particular token.
  def searchByKeywords(db: GraphDatabaseService,
    keysIn: List[String],
    maxResults: Int): Iterable[IndexSearchResult] = {
    val keys = keysIn.filter(!_.isEmpty).map(_.toLowerCase)
    val luceneQuery = keys.map{ k => s"$PropQualNameTokens: $k*"}.mkString(" AND ")
    val result = executeQuery(
      db, s"""START n=node:tpeIndex('$luceneQuery')
              MATCH n-[:containedBy*1..5]->x, n-[:containedBy*1..5]->y
              WHERE x.nodeType='file' and y.nodeType='package'
              RETURN n,x,y LIMIT $maxResults""")
    result.flatMap { row =>
      (row.get("n"), row.get("x"), row.get("y")) match {
        case (Some(tpeNode:Node), Some(fileNode:Node), Some(packNode:Node)) => {
          val sym = SymbolNode(tpeNode)
          val file = FileNode(fileNode)
          val pack = PackageNode(packNode)
          Some(SymbolSearchResult(
            pack.name + "." + sym.name, sym.localName, sym.declaredAs,
            Some((file.path, sym.offset))))
        }
        case _ => None
      }
    }.toIterable
  }

  // Find all occurances of a particular token.
  def completeTypeName(
    db: GraphDatabaseService,
    prefix:String,
    maxResults: Int): Iterable[IndexSearchResult] = {
    findTypeSuggestions(db, prefix, maxResults, None)
  }

  // Find all occurances of a particular token.
  def sourceFilesDefiningType(
    db: GraphDatabaseService,
    enclosingPackage:String,
    classNamePrefix: String): Set[File] = {
    findTypeSuggestions(
      db, classNamePrefix, 100, Some(enclosingPackage)).flatMap{
      s => s.pos.map{p => new File(p._1)}
    }.toSet
  }

  protected def findFileNodeByMd5(
    db: GraphDatabaseService, md5: String): Option[FileNode] = {
    JavaConversions.iterableAsScalaIterable(
      fileIndex.get(PropMd5, md5)).headOption.map(FileNode.apply)
  }

  protected def findFileNodeByPath(
    db: GraphDatabaseService, path: String): Option[FileNode] = {
    JavaConversions.iterableAsScalaIterable(
      fileIndex.get(PropPath,path)).headOption.map(FileNode.apply)
  }

  protected def findOrCreateFileNode(tx: DbInTransaction, f: File): FileNode = {
    val fileNode = JavaConversions.iterableAsScalaIterable(
      fileIndex.get(PropPath, f.getAbsolutePath)).headOption
    fileNode match {
      case Some(node) => FileNode(node)
      case None => {
        val node = FileNode(tx.db.createNode())
        node.setProperty(PropPath, f.getAbsolutePath)
        node.setProperty(PropNodeType, NodeTypeFile)
        fileIndex.putIfAbsent(node, PropPath, f.getAbsolutePath)
        node
      }
    }
  }

  protected def purgeFileSubgraph(tx: DbInTransaction, fileNode: FileNode) = {
    // First remove the nodes from any indexes
    executeQuery(tx.db,
      s"""START n=node:fileIndex($PropPath='${fileNode.path}')
       MATCH x-[:containedBy*1..5]->n
       RETURN x""").foreach { row =>
      row.get("x") match {
        case Some(x: Node) => {
          tpeIndex.remove(x)
          scopeIndex.remove(x)
        }
        case _ => None
      }
    }
    // Then delete all nodes and relationships.
    val result = executeQuery(tx.db,
      s"""START n=node:fileIndex($PropPath='${fileNode.path}')
       MATCH x-[:containedBy*1..5]->n
       WITH x
       MATCH x-[r]-()
       DELETE r,x""")
    println(s"Purged ${result.queryStatistics.deletedNodes} nodes.")
  }

  protected def prepareFileForNewContents(
    tx: DbInTransaction, fileNode: FileNode, md5:String): Node = {
    fileNode.md5.foreach { md5 =>
      // File had prior existence. Purge the file from the graph.
      fileIndex.remove(fileNode, PropMd5, md5)
      purgeFileSubgraph(tx, fileNode)
    }
    // Stamp with new contents fingerprint. Ready for new subgraph.
    fileNode.setProperty(PropMd5, md5)
    fileIndex.add(fileNode, PropMd5, md5)
    fileNode
  }

  def unindex(files: Set[File]) {
    doTx { tx =>
      files.foreach { f =>
        findFileNodeByPath(tx.db, f.getAbsolutePath) match {
          case Some(fileNode) => {
            purgeFileSubgraph(tx, fileNode)
            fileNode.delete()
          }
          case _ => {}
        }
      }
    }
  }

  def index(files: Set[File]) {
    val MaxCompilers = 5
    val MaxWorkers = 5

    class RoundRobin[T](items: IndexedSeq[T]) {
      var i = 0
      def next():T = {
        val item = items(i)
        i = (i + 1) % items.length
        item
      }
      def foreach(action: T => Unit) = items.foreach(action)
    }

    val compilers = new RoundRobin(
      (0 until MaxCompilers).map{i => newCompiler()})

    val workers = new RoundRobin(
      (0 until MaxWorkers).map{i =>
        val w = new SourceParsingWorker(compilers.next())
        w.start()
      })

    val bytecodeWorker = new BytecodeWorker(filterPredicate)
    bytecodeWorker.start()

    def workerForFile(f:File): Option[Actor] = {
      if (f.getName.endsWith(".java") || f.getName.endsWith(".scala")) {
        Some(workers.next())
      } else if (f.getName.endsWith(".class") || f.getName.endsWith(".jar")) {
        Some(bytecodeWorker)
      } else {
        System.err.println("PIG: Unrecognized file type: " + f)
        None
      }
    }

    val futures = files.flatMap { f =>
      val md5 = FileUtils.md5(f)
      findFileNodeByMd5(graphDb, md5) match {
        case Some(fileNode) => {
          if (fileNode.path != f.getAbsolutePath) {
            // File contents are identical, but name has changed.
            doTx { tx =>
              fileNode.setProperty(PropPath, f.getAbsolutePath)
            }
          }
          None
        }
        // File contents not found.
        case None => workerForFile(f).map{ w => w !! ParseFile(f, md5) }
      }
    }
    val results = futures.map{f => f()}
    compilers.foreach{ _.askShutdown() }
    workers.foreach { _ ! Die() }
  }

  def indexAll(files: Iterable[File]) {
    index(FileUtils.expandRecursively(
      projectRoot, files, {f:File => true}).toSet)
  }

}

object PIG extends PIGIndex {
  var graphDb: GraphDatabaseService = createDefaultGraphDb
  var fileIndex: Index[Node] = createDefaultFileIndex(graphDb)
  var tpeIndex: Index[Node] = createDefaultTypeIndex(graphDb)
  var scopeIndex: Index[Node] = createDefaultScopeIndex(graphDb)
  var scalaLibraryJar: File = new File("dist_2.10.0/lib/scala-library.jar")
  var projectRoot: File = new File(".")

  def main(args: Array[String]) {
    System.setProperty("actors.corePoolSize", "20")
    System.setProperty("actors.maxPoolSize", "100")
    Profiling.time("Index all roots") {
      indexAll(args.map(new File(_)))
    }
    Util.foreachInputLine { line =>
      val name = line
      for (l <- findTypeSuggestions(graphDb, name, 20, None)) {
        println(l)
      }
    }
    graphDb.shutdown()
  }

}
