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

package org.ensime.indexer {
  import java.io.File
  import org.ensime.server.PIGIndex
  import org.neo4j.graphdb.{GraphDatabaseService, Node}
  import scala.actors._
  import scala.actors.Actor._
  import scala.collection.mutable.{ArrayStack, HashSet}
  import scala.tools.nsc.Settings
  import scala.tools.nsc.interactive.{CompilerControl, Global}
  import scala.tools.nsc.reporters.{Reporter, StoreReporter}
  import scala.tools.nsc.util.SourceFile
  import org.ensime.util._
  import org.objectweb.asm.Opcodes
  import scala.util.matching.Regex

  trait IndexProducers { self: PIGIndex =>

    case class Die()
    case class ParseFile(f: File, md5: String)

    class BytecodeWorker(nameFilter: (String => Boolean))
        extends Actor {

      class ClassVisitor(fileNode: FileNode, tx: DbInTransaction,
        nameFilter: (String => Boolean))
          extends ClassHandler {

        def isValidType(s: String): Boolean = {
          val i = s.indexOf("$")
          i == -1 || (i == (s.length - 1))
        }

        def isValidMethod(s: String): Boolean = {
          s.indexOf("$") == -1 && !s.equals("<init>") && !s.equals("this")
        }

        private def declaredAs(name: String, flags: Int) = {
          if (name.endsWith("$")) "object"
          else if ((flags & Opcodes.ACC_INTERFACE) != 0) "trait"
          else "class"
        }

        var classCount = 0
        var methodCount = 0
        var currentClassNode: Option[Node] = None

        override def onClass(
          qualName: String, localName:String, location: String, flags: Int) {
          val isPublic = ((flags & Opcodes.ACC_PUBLIC) != 0)
          if (isPublic && isValidType(qualName) && nameFilter(qualName)) {

            val packNode = PackageNode.create(
              tx, qualName.substring(0, qualName.lastIndexOf(".")),
              0, fileNode)

            val node = tx.db.createNode()
            node.setProperty(PropNodeType, NodeTypeType)
            node.setProperty(PropName, localName)
            node.setProperty(PropDeclaredAs, declaredAs(localName, flags))
            node.createRelationshipTo(packNode, RelContainedBy)

            tpeIndex.add(node, PropNameTokens,
              Tokens.tokenizeCamelCaseName(localName))
            tpeIndex.add(node, PropQualNameTokens,
              Tokens.tokenizeCamelCaseName(qualName))

            currentClassNode = Some(node)
          }
        }

        override def onClassEnd() {
          for (node <- currentClassNode) {
            currentClassNode = None
            classCount += 1
          }
        }

        override def onMethod(className: String, name: String,
          location: String, flags: Int) {
          val isPublic = ((flags & Opcodes.ACC_PUBLIC) != 0)
          if (isPublic && isValidMethod(name)) {
            currentClassNode.map { classNode =>
              val node = tx.db.createNode()
              node.setProperty(PropNodeType, NodeTypeMethod)
              node.setProperty(PropName, name)
              node.createRelationshipTo(classNode, RelContainedBy)
              methodCount += 1
            }
          }
        }
      }

      def act() {
        loop {
          receive {
            case ParseFile(f: File, md5: String) => {
              doTx { tx =>
                println("Adding bytecode " + f)
                val fileNode = findOrCreateFileNode(tx, f)
                prepareFileForNewContents(tx, fileNode, md5)
                val visitor = new ClassVisitor(fileNode, tx, nameFilter)
                ClassIterator.findPublicSymbols(List(f), visitor)
              }
              reply(f)
            }
            case Die() => exit()
          }
        }
      }
    }

    class SourceParsingWorker(compiler:Global with CompilerExtensions) extends Actor {
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

    def newCompiler(): Global with CompilerExtensions = {
      val settings = new Settings(Console.println)
      settings.usejavacp.value = false
      settings.classpath.value = scalaLibraryJar.getAbsolutePath
      val reporter = new StoreReporter()
      new Global(settings, reporter) with CompilerExtensions {}
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

        var currentPackage: Option[String] = None

        override def traverse(t: Tree) {
          val treeP = t.pos
          if (!treeP.isTransparent) {
            try {
              t match {
                case PackageDef(pid, stats) => {
                  val node = tx.db.createNode()
                  node.setProperty(PropNodeType, NodeTypePackage)
                  val fullName = pid.toString
                  node.setProperty(PropName, fullName)
                  node.setProperty(PropOffset, treeP.startOrPoint)
                  tokenStack.top += fullName.toLowerCase
                  node.createRelationshipTo(stack.top, RelContainedBy)
                  currentPackage = Some(fullName)
                  descendWithContext(t, node)
                  currentPackage = None
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
                  currentPackage.foreach {
                    p => tpeIndex.add(node, PropQualNameTokens,
                      Tokens.tokenizeCamelCaseName(p + "." + localName))}
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
                  currentPackage.foreach{
                    p => tpeIndex.add(node, PropQualNameTokens,
                      Tokens.tokenizeCamelCaseName(p + "." + localName))}
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
  }
}
