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
import org.ensime.model.{ImportSuggestions, SymbolSearchResults}
import org.ensime.protocol.ProtocolConst._
import org.ensime.protocol.ProtocolConversions
import org.ensime.util._
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.index.Index
import scala.actors._
import scala.actors.Actor._

case class IndexerShutdownReq()
case class RebuildStaticIndexReq()
case class TypeCompletionsReq(prefix: String, maxResults: Int)
case class SourceFileCandidatesReq(enclosingPackage: String,
  classNamePrefix: String)
case class IndexFilesReq(files: Iterable[File])
case class UnindexFilesReq(files: Iterable[File])

/**
  * The main index actor.
  */
class Indexer(
  project: Project,
  protocol: ProtocolConversions,
  config: ProjectConfig) extends Actor with PIGIndex {

  import protocol._
  var graphDb: GraphDatabaseService = null
  var fileIndex: Index[Node] = null
  var tpeIndex: Index[Node] = null
  var scopeIndex: Index[Node] = null
  var scalaLibraryJar: File =
    config.scalaLibraryJar.getOrElse(
      throw new RuntimeException(
        "Indexer requires that the scala library jar be specified."))

  protected def projectRoot: File = config.root

  override def filterPredicate(qualifiedName:String):Boolean = {
    if (config.onlyIncludeInIndex.isEmpty ||
      config.onlyIncludeInIndex.exists(_.findFirstIn(qualifiedName) != None)) {
      config.excludeFromIndex.forall(_.findFirstIn(qualifiedName) == None)
    } else {
      false
    }
  }

  def act() {
    val factory = new GraphDatabaseFactory()
    graphDb = createDefaultGraphDb
    fileIndex = createDefaultFileIndex(graphDb)
    tpeIndex = createDefaultTypeIndex(graphDb)
    scopeIndex = createDefaultScopeIndex(graphDb)
    loop {
      try {
        receive {
          case IndexerShutdownReq() => {
            graphDb.shutdown()
            exit('stop)
          }
          case RebuildStaticIndexReq() => {
            Profiling.time("Index all source roots") {
              indexAll(
                config.allFilesOnClasspath ++
                  config.sourceRoots ++
                  config.referenceSourceRoots)
            }
            project ! AsyncEvent(toWF(IndexerReadyEvent()))
          }
          case IndexFilesReq(files: Iterable[File]) => {
            index(files.toSet)
          }
          case UnindexFilesReq(files: Iterable[File]) => {
            unindex(files.toSet)
          }
          case TypeCompletionsReq(prefix: String, maxResults: Int) => {
            sender ! completeTypeName(graphDb, prefix, maxResults)
          }
          case SourceFileCandidatesReq(enclosingPackage, classNamePrefix) => {
            sender ! sourceFilesDefiningType(
              graphDb, enclosingPackage, classNamePrefix)
          }
          case RPCRequestEvent(req: Any, callId: Int) => {
            try {
              req match {
                case ImportSuggestionsReq(file: File, point: Int,
                  names: List[String], maxResults: Int) => {
                  val suggestions = ImportSuggestions(
                    names.map(
                      findTypeSuggestions(graphDb, _, maxResults, None)))
                  project ! RPCResultEvent(toWF(suggestions), callId)
                }
                case PublicSymbolSearchReq(keywords: List[String],
                  maxResults: Int) => {
                  val suggestions = SymbolSearchResults(
                    searchByKeywords(graphDb, keywords, maxResults))
                  project ! RPCResultEvent(toWF(suggestions), callId)
                }
              }
            } catch {
              case e: Exception =>
                {
                  System.err.println("Error handling RPC: " +
                    e + " :\n" +
                    e.getStackTraceString)
                  project.sendRPCError(ErrExceptionInIndexer,
                    Some("Error occurred in indexer. Check the server log."),
                    callId)
                }
            }
          }
          case other =>
            {
              println("Indexer: WTF, what's " + other)
            }
        }

      } catch {
        case e: Exception => {
          System.err.println("Error at Indexer message loop: " +
            e + " :\n" + e.getStackTraceString)
        }
      }
    }
  }

  override def finalize() {
    System.out.println("Finalizing Indexer actor.")
  }
}
