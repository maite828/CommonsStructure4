@Library('libpipelines@master') _

config {
    repoUrl = 'http://bitbucket.es.wcorp.carrefour.com/scm/fw/commons.git'


    repoConfigUrl = ''
    environmentURL = [
      desa : '',
      cua  : '',
      pro  : '',
    ]

    deployableArtifactId = 'commons.Assembly'
    deployableArtifactExtension = '-v1.tar.gz'


    buildTool = [
      name : 'maven',
      version : '3.3.9',
    ]
}

doInitTools()
doGit()
doBuild()
doUnitTests()
doIntegrationTest()
doUploadArtifacts()
doDeploy()
doAceptanceTest()
