# Load Testing

> Comment peut on mettre en place des tests de charges dans la CI/CD de GitLab ?

Il nous faut les 3 composants suivants :

* Une instance du service
* Un outil de test de charge
* Un service de monitoring

## Outils

Liens utiles :

* https://docs.gitlab.com/ee/ci/testing/load_performance_testing.html
* https://k6.io/blog/integrating-load-testing-with-gitlab/
* https://docs.gitlab.com/ee/ci/testing/metrics_reports.html
* https://gatling.io/
* https://github.com/tsenart/vegeta
* https://github.com/ddosify/ddosify
* ...

Quelques outils pour l'intégration dans la CI de GitLab :

* Artillery.io
* gatling.io
* K6 / K6.cloud
* ddosify
* vegeta
* go-wrk <!-- il ne gère pas les reponses en 204 comme statut OK -->
* ...

## Monitoring

Quels sont les outils qui propose un monitoring des tests de charge sur la CI/CD de GitLab ?

* ...

## Instance

Comment deployer une instance sur la CI ?

> deploiement sur Azure ?
