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
import org.ensime.config.ProjectConfig
import org.ensime.indexer.Tokens
import org.ensime.model.ImportSuggestions
import org.ensime.model.IndexSearchResult
import org.ensime.model.SymbolSearchResult
import org.ensime.model.TypeSearchResult
import org.ensime.protocol.ProtocolConversions
import org.ensime.util.{FileUtils, Profiling, StringSimilarity, Util}
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.cypher.ExecutionResult
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.{DynamicRelationshipType, GraphDatabaseService, Node}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.index.{Index, IndexHits, IndexManager}
import org.neo4j.helpers.collection.MapUtil
import scala.actors._
import scala.actors.Actor._
import scala.collection.{Iterable, JavaConversions}
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.{ArrayStack, HashMap, HashSet}
import scala.tools.nsc.Settings
import scala.tools.nsc.interactive.{CompilerControl, Global}
import scala.tools.nsc.reporters.{Reporter, StoreReporter}
import scala.tools.nsc.util.SourceFile

class RoundRobin[T](items: IndexedSeq[T]) {
  var i = 0
  def next():T = {
    val item = items(i)
    i = (i + 1) % items.length
    item
  }
  def foreach(action: T => Unit) = items.foreach(action)
}

trait PIGIndex extends StringSimilarity {

  private val cache = new HashMap[(String, String), Int]
  def editDist(a: String, b: String): Int = {
    cache.getOrElseUpdate((a, b), getLevenshteinDistance(a, b))
  }

  val PropName = "name"
  val PropNameTokens = "nameTokens"
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

  protected def createDefaultGraphDb =
    (new GraphDatabaseFactory()).newEmbeddedDatabase("neo4j-db")

  protected def createDefaultFileIndex(db: GraphDatabaseService) =
    db.index().forNodes("fileIndex")

  protected def createDefaultTypeIndex(db: GraphDatabaseService) =
    db.index().forNodes("tpeIndex",
      MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ))

  protected def createDefaultScopeIndex(db: GraphDatabaseService) =
    db.index().forNodes("scopeIndex",
      MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ))

  protected def shutdown = { graphDb.shutdown }

  private def doTx(f: DbInTransaction => Unit): Unit = {
    val tx = DbInTransaction(graphDb, graphDb.beginTx())
    try {
      f(tx)
      tx.transaction.success()
    } finally {
      tx.transaction.finish()
    }
  }

  private def executeQuery(
      db: GraphDatabaseService, q: String): ExecutionResult = {
    val engine = new ExecutionEngine(graphDb)
    engine.execute(q)
  }

  // Find all types with names similar to typeName.
  def findTypeSuggestions(db: GraphDatabaseService,
    typeName:String, maxResults: Int): Iterable[TypeSearchResult] = {
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
          Some(TypeSearchResult(
            pack.name + "." + tpe.name, tpe.localName, tpe.declaredAs,
            Some((file.path, tpe.offset))))
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

  private def findFileNodeByMd5(
      db: GraphDatabaseService, md5: String): Option[FileNode] = {
    JavaConversions.iterableAsScalaIterable(
      fileIndex.get(PropMd5, md5)).headOption.map(FileNode.apply)
  }

  private def findOrCreateFileNode(tx: DbInTransaction, f: File): FileNode = {
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

  private def purgeFileSubgraph(tx: DbInTransaction, fileNode: FileNode) = {
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

  private def prepareFileForNewContents(
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

  trait CompilerExtensions { self: Global =>
    import scala.tools.nsc.symtab.Flags._
    import scala.tools.nsc.util.RangePosition

    // See mads379's 8480b638c785c504e09b4fb829acdb24117af0c2
    def quickParse(source: SourceFile): Tree = {
      import syntaxAnalyzer.UnitParser
      new UnitParser(new CompilationUnit(source)).parse()
    }

    class TreeTraverser(fileNode: Node, tx: DbInTransaction)
        extends Traverser {
      // A stack of nodes corresponding to the syntactic nesting as we traverse
      // the AST.
      val stack = ArrayStack[Node]()
      stack.push(fileNode)

      // A stack of token blobs, to be indexed with the corresponding node.
      val tokenStack = ArrayStack[HashSet[String]]()
      tokenStack.push(HashSet[String]())

      def descendWithContext(t: Tree, containingNode: Node) {
        tokenStack.push(HashSet[String]())
        stack.push(containingNode)
        super.traverse(t)
        stack.pop()
        val tokens = tokenStack.pop()
        scopeIndex.add(containingNode, PropNameTokens, tokens.mkString(" "))
      }

      override def traverse(t: Tree) {
        val treeP = t.pos
        if (!treeP.isTransparent) {
          try {
            t match {
              case PackageDef(pid, stats) => {
                val node = tx.db.createNode()
                node.setProperty(PropNodeType, NodeTypePackage)
                val fullName = pid.qualifier.toString + "." + pid.name.decode
                node.setProperty(PropName, fullName)
                node.setProperty(PropOffset, treeP.startOrPoint)
                tokenStack.top += fullName.toLowerCase
                node.createRelationshipTo(stack.top, RelContainedBy)
                descendWithContext(t, node)
              }

              case Import(expr, selectors) => {
                for (impSel <- selectors) {
                  val node = tx.db.createNode()
                  node.setProperty(PropNodeType, NodeTypeImport)
                  val importedName = impSel.name.decode
                  node.setProperty(PropName, importedName)
                  node.setProperty(PropOffset, treeP.startOrPoint)
                  tokenStack.top + importedName
                  node.createRelationshipTo(stack.top, RelContainedBy)
                }
              }

              case ClassDef(mods, name, tparams, impl) => {
                val localName = name.decode
                val node = tx.db.createNode()
                node.setProperty(PropNodeType, NodeTypeType)
                node.setProperty(PropName, localName)
                node.setProperty(PropDeclaredAs,
                  if (mods.isTrait) "trait"
                  else if (mods.isInterface) "interface"
                  else "class")
                node.setProperty(PropOffset, treeP.startOrPoint)
                node.createRelationshipTo(stack.top, RelContainedBy)
                tpeIndex.add(
                  node, PropNameTokens, Tokens.tokenizeCamelCaseName(localName))
                tokenStack.top += localName
                descendWithContext(t, node)
              }

              case ModuleDef(mods, name, impl) => {
                val localName = name.decode
                val node = tx.db.createNode()
                node.setProperty(PropNodeType, NodeTypeType)
                node.setProperty(PropName, name.decode)
                node.setProperty(PropDeclaredAs, "object")
                node.setProperty(PropOffset, treeP.startOrPoint)
                node.createRelationshipTo(stack.top, RelContainedBy)
                tpeIndex.add(
                  node, PropNameTokens, Tokens.tokenizeCamelCaseName(localName))
                tokenStack.top += localName
                descendWithContext(t, node)
              }

              case ValDef(mods, name, tpt, rhs) => {
                val node = tx.db.createNode()
                node.setProperty(PropNodeType, NodeTypeType)
                node.setProperty(PropName, name.decode)
                node.createRelationshipTo(stack.top, RelContainedBy)
                node.setProperty(PropOffset, treeP.startOrPoint)
                tokenStack.top += name.decode
                val isField =
                  stack.top.getProperty(PropNodeType) == NodeTypeType
                if (mods.hasFlag(PARAM)) {
                  node.setProperty(PropNodeType, NodeTypeParam)
                  node.createRelationshipTo(stack.top, RelParamOf)
                } else if (mods.hasFlag(MUTABLE) && !isField) {
                  node.setProperty(PropNodeType, NodeTypeVarDef)
                } else if (!isField) {
                  node.setProperty(PropNodeType, NodeTypeValDef)
                } else if (mods.hasFlag(MUTABLE) && isField) {
                  node.setProperty(PropNodeType, NodeTypeVarField)
                } else if (isField) {
                  node.setProperty(PropNodeType, NodeTypeValField)
                }
                descendWithContext(t, node)
              }

              case DefDef(mods, name, tparams, vparamss, tpt, rhs) => {
                val node = tx.db.createNode()
                node.setProperty(PropNodeType, NodeTypeMethod)
                node.setProperty(PropName, name.decode)
                node.setProperty(PropOffset, treeP.startOrPoint)
                node.createRelationshipTo(stack.top, RelContainedBy)
                descendWithContext(t, node)
              }

              case TypeDef(mods, name, tparams, rhs) => {
                tokenStack.top += name.decode
                super.traverse(t)
              }

              case LabelDef(name, params, rhs) => {}

              case Ident(name) => {
                tokenStack.top += name.decode
              }

              case Select(qual, selector: Name) => {
                tokenStack.top += selector.decode
                super.traverse(t)
              }

              case _ => { super.traverse(t) }
            }
          }
          catch{
            case e : Throwable => {
              System.err.println("Error in AST traverse:")
              e.printStackTrace(System.err)
            }
          }
        }
      }
    }
  }

  case class Die()
  case class ParseFile(f: File, md5: String)
  class Worker(compiler:Global with CompilerExtensions) extends Actor {
    def act() {
      loop {
        receive {
          case ParseFile(f: File, md5: String) => {
            doTx { tx =>
              // Get existing file if present (contents may have been updated):
              val fileNode = findOrCreateFileNode(tx, f)
              prepareFileForNewContents(tx, fileNode, md5)
              val sf = compiler.getSourceFile(f.getAbsolutePath())
              println("Adding " + f)
              val tree = compiler.quickParse(sf)
              val traverser = new compiler.TreeTraverser(fileNode, tx)
              traverser.traverse(tree)
            }
            reply(f)
          }
          case Die() => exit()
        }
      }
    }
  }

  def index(files: Set[File]) {
    val MaxCompilers = 5
    val MaxWorkers = 5

    def newCompiler(): Global with CompilerExtensions = {
      val settings = new Settings(Console.println)
      settings.usejavacp.value = false
      settings.classpath.value = scalaLibraryJar.getAbsolutePath
      val reporter = new StoreReporter()
      new Global(settings, reporter) with CompilerExtensions {}
    }

    val compilers = new RoundRobin(
      (0 until MaxCompilers).map{i => newCompiler()})

    val workers = new RoundRobin(
      (0 until MaxWorkers).map{i =>
        val w = new Worker(compilers.next())
        w.start()
      })

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
        case None => Some(workers.next() !! ParseFile(f, md5))
      }
    }
    val results = futures.map{f => f()}
    compilers.foreach{ _.askShutdown() }
    workers.foreach { _ ! Die() }
  }

  def indexDirectories(sourceRoots: Iterable[File]) {
    index(FileUtils.expandRecursively(
      new File("."), sourceRoots, FileUtils.isValidSourceFile _).toSet)
  }

}


case class ShutdownReq()

class PIG(
  project: Project,
  protocol: ProtocolConversions,
  config: ProjectConfig) extends Actor with PIGIndex{

  import org.ensime.protocol.ProtocolConst._
  import protocol._

  var graphDb: GraphDatabaseService = null
  var fileIndex: Index[Node] = null
  var tpeIndex: Index[Node] = null
  var scopeIndex: Index[Node] = null
  var scalaLibraryJar: File =
    config.scalaLibraryJar.getOrElse(
      throw new RuntimeException(
        "PIG requires that the scala library jar be specified."))

  def act() {
    val factory = new GraphDatabaseFactory()
    graphDb = createDefaultGraphDb
    fileIndex = createDefaultFileIndex(graphDb)
    tpeIndex = createDefaultTypeIndex(graphDb)
    scopeIndex = createDefaultScopeIndex(graphDb)
    indexDirectories(config.sourceRoots)
    loop {
      try {
        receive {
          case ShutdownReq() => {
            graphDb.shutdown()
            exit('stop)
          }
          case RPCRequestEvent(req: Any, callId: Int) => {
            try {
              req match {
                case ImportSuggestionsReq(
                  file: File,
                  point: Int,
                  names: List[String],
                  maxResults: Int) => {
                  val suggestions = ImportSuggestions(
                    names.map(findTypeSuggestions(graphDb, _, maxResults)))
                  project ! RPCResultEvent(toWF(suggestions), callId)
                }
                case PublicSymbolSearchReq(
                  keywords: List[String],
                  maxResults: Int) => {
                  //                  val suggestions = SymbolSearchResults()
                  //                  project ! RPCResultEvent(toWF(suggestions), callId)
                }
              }
            } catch {
              case e: Exception =>
                {
                  System.err.println("Error handling RPC: " +
                    e + " :\n" + e.getStackTraceString)
                  project.sendRPCError(ErrExceptionInIndexer,
                    Some("Error occurred in PIG. Check the server log."),
                    callId)
                }
            }
          }
          case other =>
            {
              println("PIG: WTF, what's " + other)
            }
        }

      } catch {
        case e: Exception => {
          System.err.println("Error at PIG message loop: " +
            e + " :\n" + e.getStackTraceString)
        }
      }
    }
  }

  override def finalize() {
    System.out.println("Finalizing Indexer actor.")
  }

}

object PIG extends PIGIndex {
  var graphDb: GraphDatabaseService = createDefaultGraphDb
  var fileIndex: Index[Node] = createDefaultFileIndex(graphDb)
  var tpeIndex: Index[Node] = createDefaultTypeIndex(graphDb)
  var scopeIndex: Index[Node] = createDefaultScopeIndex(graphDb)
  var scalaLibraryJar: File = new File("dist_2.10.0/lib/scala-library.jar")

  def main(args: Array[String]) {
    System.setProperty("actors.corePoolSize", "10")
    System.setProperty("actors.maxPoolSize", "100")
    Profiling.time {
      indexDirectories(args.map(new File(_)))
    }
    Util.foreachInputLine { line =>
      val name = line
      for (l <- findTypeSuggestions(graphDb, name, 20)) {
        println(l)
      }
    }
    graphDb.shutdown()
  }

}
