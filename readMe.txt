
CREARE LE CARTELLE:
logs
cfg
nella root HCpy

(sono in .gitIgnore)


in cfg creare file:
cfg.ini

contenuto:

[email]
emails_from=< mittente delle email di sistema. es: replies-disabled@cloud-SAS.com >

[app client]
basic_authorization=< la basic auth con utente/pw del client API encodate in base 64 come fa postman >
refresh_token=< il refresh token del client, ottenuto da una chiamata di tipo password >

[default client]
username=< username viya >
password=< pw di utenza viya >
basic_authorization=< la basic auth con utente/pw del client API encodate in base 64 come fa postman >

[jobs]
jobexec_pgm_from_url=<path nel content della job execution per lanciare pgm generico da url, es.%2FSNM%2Futility_jobs%2Fexec_pgm_from_url>



# vs code, da git bash:
git config --global user.name "marcoZav"
git config --global user.email "marco.zavarini@gmail.com"

