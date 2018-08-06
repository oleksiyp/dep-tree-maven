package run

import org.apache.maven.artifact.DefaultArtifact
import org.apache.maven.artifact.repository.ArtifactRepository
import org.apache.maven.artifact.repository.ArtifactRepositoryPolicy
import org.apache.maven.artifact.repository.MavenArtifactRepository
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout
import org.apache.maven.execution.*
import org.apache.maven.lifecycle.internal.MojoDescriptorCreator
import org.apache.maven.model.Dependency
import org.apache.maven.plugin.Mojo
import org.apache.maven.plugin.MojoExecution
import org.apache.maven.plugin.PluginParameterExpressionEvaluator
import org.apache.maven.plugin.descriptor.MojoDescriptor
import org.apache.maven.plugin.descriptor.Parameter
import org.apache.maven.plugin.descriptor.PluginDescriptorBuilder
import org.apache.maven.plugins.dependency.tree.TreeMojo
import org.apache.maven.project.MavenProject
import org.apache.maven.repository.RepositorySystem
import org.apache.maven.repository.internal.MavenRepositorySystemSession
import org.codehaus.plexus.*
import org.codehaus.plexus.classworlds.ClassWorld
import org.codehaus.plexus.component.configurator.ComponentConfigurator
import org.codehaus.plexus.component.repository.ComponentDescriptor
import org.codehaus.plexus.configuration.xml.XmlPlexusConfiguration
import org.codehaus.plexus.util.InterpolationFilterReader
import org.codehaus.plexus.util.ReaderFactory
import org.codehaus.plexus.util.StringUtils
import org.codehaus.plexus.util.xml.Xpp3Dom
import org.sonatype.aether.impl.internal.SimpleLocalRepositoryManager

import java.io.*
import java.util.*

class Run(baseDir: String) {
    private val classWorld = ClassWorld("plexus.core", Thread.currentThread().contextClassLoader)
    private val containerConfiguration = DefaultContainerConfiguration().setClassWorld(classWorld).setName("embedder")

    val container = DefaultPlexusContainer(containerConfiguration)

    private val configurator = container.lookup(ComponentConfigurator::class.java, "basic")
    private val mojoDescriptors = mutableMapOf<String, MojoDescriptor>()

    init {
        val inputStream = javaClass.getResourceAsStream("/META-INF/maven/plugin.xml")
        val reader = ReaderFactory.newXmlReader(inputStream)
        val interpolationFilterReader = InterpolationFilterReader(
            BufferedReader(reader),
            container.context.contextData as Map<String, Any>
        )

        val pluginDescriptor = PluginDescriptorBuilder().build(interpolationFilterReader)
        val repositorySystem = container.lookup(RepositorySystem::class.java)
        val artifact = repositorySystem.createArtifact(
            pluginDescriptor.groupId,
            pluginDescriptor.artifactId,
            pluginDescriptor.version,
            ".jar"
        )
        artifact.file = File(baseDir).canonicalFile
        pluginDescriptor.pluginArtifact = artifact
        pluginDescriptor.artifacts = Arrays.asList(artifact)

        pluginDescriptor.components.forEach { desc ->
            container.addComponentDescriptor(desc)
        }

        pluginDescriptor.mojos.forEach { mojoDescriptor ->
            mojoDescriptors[mojoDescriptor.goal] = mojoDescriptor
        }
    }

    @Throws(Exception::class)
    fun lookupConfiguredMojo(session: MavenSession, execution: MojoExecution): Mojo {
        val project = session.currentProject
        val mojoDescriptor = execution.mojoDescriptor
        val mojo = container.lookup(mojoDescriptor.role, mojoDescriptor.roleHint) as Mojo
        val evaluator = PluginParameterExpressionEvaluator(session, execution)
        var configuration: Xpp3Dom? = null
        val plugin = project.getPlugin(mojoDescriptor.pluginDescriptor.pluginLookupKey)
        if (plugin != null) {
            configuration = plugin.configuration as Xpp3Dom
        }

        if (configuration == null) {
            configuration = Xpp3Dom("configuration")
        }

        configuration = Xpp3Dom.mergeXpp3Dom(execution.configuration, configuration)
        val pluginConfiguration = XmlPlexusConfiguration(configuration)
        configurator.configureComponent(mojo, pluginConfiguration, evaluator, container.containerRealm)
        return mojo
    }

    fun newMojoExecution(goal: String): MojoExecution {
        val mojoDescriptor = mojoDescriptors[goal] as MojoDescriptor
        val execution = MojoExecution(mojoDescriptor)
        finalizeMojoConfiguration(execution)
        return execution
    }

    private fun finalizeMojoConfiguration(mojoExecution: MojoExecution) {
        val mojoDescriptor = mojoExecution.mojoDescriptor
        var executionConfiguration: Xpp3Dom? = mojoExecution.configuration
        if (executionConfiguration == null) {
            executionConfiguration = Xpp3Dom("configuration")
        }

        val defaultConfiguration = MojoDescriptorCreator.convert(mojoDescriptor)
        val finalConfiguration = Xpp3Dom("configuration")
        if (mojoDescriptor.parameters != null) {
            val `i$` = mojoDescriptor.parameters.iterator()

            while (`i$`.hasNext()) {
                val parameter = `i$`.next() as Parameter
                var parameterConfiguration: Xpp3Dom? = executionConfiguration.getChild(parameter.name)
                if (parameterConfiguration == null) {
                    parameterConfiguration = executionConfiguration.getChild(parameter.alias)
                }

                val parameterDefaults = defaultConfiguration.getChild(parameter.name)
                parameterConfiguration =
                        Xpp3Dom.mergeXpp3Dom(parameterConfiguration, parameterDefaults, java.lang.Boolean.TRUE)
                if (parameterConfiguration != null) {
                    parameterConfiguration = Xpp3Dom(parameterConfiguration, parameter.name)
                    if (StringUtils.isEmpty(parameterConfiguration.getAttribute("implementation")) && StringUtils.isNotEmpty(
                            parameter.implementation
                        )
                    ) {
                        parameterConfiguration.setAttribute("implementation", parameter.implementation)
                    }

                    finalConfiguration.addChild(parameterConfiguration)
                }
            }
        }

        mojoExecution.configuration = finalConfiguration
    }


    fun stop() {
        container.dispose()
    }

}


fun main(args: Array<String>) {
    val baseDir = File("").absolutePath
    val run = Run(baseDir)
    try {
        val project = MavenProject()
        project.artifact = DefaultArtifact("abc", "def", "1.0", "compile", "jar", "", null)
        val dep = Dependency()
        dep.groupId = "io.mockk"
        dep.artifactId = "mockk"
        dep.version = "1.8.6"
        dep.scope = "test"
        project.dependencies = listOf(dep)

        val request = DefaultMavenExecutionRequest()

        val repo = MavenArtifactRepository(
            "id",
            "file:////home/oleksiyp/workspace/dep-tree-maven/repo",
            DefaultRepositoryLayout(),
            ArtifactRepositoryPolicy(true, "always", "never"),
            ArtifactRepositoryPolicy(true, "always", "never")
        )
        request.localRepository = repo
        val mavenCentral = MavenArtifactRepository(
            "id",
            "https://repo.maven.apache.org/maven2/",
            DefaultRepositoryLayout(),
            ArtifactRepositoryPolicy(true, "always", "never"),
            ArtifactRepositoryPolicy(true, "always", "never")
        )
        request.remoteRepositories = listOf<ArtifactRepository>(mavenCentral)
        project.remoteArtifactRepositories = listOf<ArtifactRepository>(mavenCentral)


        val result = DefaultMavenExecutionResult()
        val repositorySystemSession = MavenRepositorySystemSession()

        repositorySystemSession.localRepositoryManager = SimpleLocalRepositoryManager(
            File("/home/oleksiyp/workspace/dep-tree-maven/repo")
        )

        val session = MavenSession(run.container, repositorySystemSession, request, result)

        session.currentProject = project
        session.projects = listOf(project)
        val mojoExecution = run.newMojoExecution("tree")

        val mojo = run.lookupConfiguredMojo(session, mojoExecution) as TreeMojo

        mojo.execute()

        val rootNode = mojo.dependencyGraph
        println(rootNode)
    } catch (ex: Exception) {
        throw RuntimeException(ex)
    } finally {
        run.stop()
    }
}
