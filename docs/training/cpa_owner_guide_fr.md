# Guide du propriétaire de cabinet CPA — OtoCPA

Ce guide s'adresse à la personne qui configure le cabinet, ajoute les
clients, gère les employés et supervise toutes les opérations. Si vous
êtes CPA employé(e) (et non propriétaire), voir plutôt
[cpa_employee_guide_fr.md](cpa_employee_guide_fr.md).

## 1. Configuration initiale

1. Recevez votre courriel de bienvenue et cliquez sur **Configurer mon
   compte**.
2. Définissez un mot de passe d'au moins 10 caractères (1 chiffre et
   1 lettre minimum).
3. Activez l'**authentification à deux facteurs** (obligatoire pour
   les propriétaires). Utilisez une application comme Google
   Authenticator ou Authy.
4. Complétez votre profil : nom d'affichage, numéro WhatsApp (si vous
   voulez recevoir les alertes), langue préférée.

## 2. Ajouter des clients

Menu **Clients → + Nouveau client**. Chaque client a :

- un **code client** unique (utilisé comme identifiant technique),
- un **nom d'affichage** (visible sur le tableau de bord),
- un **courriel de contact** (pour les envois automatiques),
- une **fin d'exercice** (pour la clôture mensuelle et les
  déclarations),
- un **mode portail** : *single* (lien partagé) ou *multi*
  (invitations personnelles).

## 3. Activer le portail multi-utilisateur

Pour chaque client dont l'équipe comptable compte plus d'une personne :

1. Ouvrir la fiche client.
2. Cliquer **Mode portail → Multi**.
3. Le premier utilisateur administrateur est créé automatiquement
   à partir du courriel de contact; il reçoit un lien personnel.
4. Il peut ensuite inviter le reste de son équipe via son portail.

**Avantage** : chaque téléversement est étiqueté avec l'auteur; vous
filtrez la file par personne avec le menu déroulant *Téléversé par*.

## 4. Gérer votre équipe interne

Menu **Paramètres → Employés** :

- **Inviter un nouvel employé** : entrez son courriel + rôle (admin
  / reviewer / bookkeeper).
- **Suspendre** un employé (accès bloqué mais historique préservé).
- **Retirer** un employé (jetons invalidés immédiatement).
- **Changer le rôle** d'un employé existant.

## 5. File de révision

Le tableau **Accueil** présente tous les documents en attente :

- **Filtrer** par client, par statut, par période, ou par
  téléverseur (menu déroulant avec pastilles colorées).
- **Attribuer** un document à un employé : bouton *Attribuer*.
- **Approuver / Rejeter / Faire une escalade** pour chaque document.
- **Approbation en lot** via la case à cocher dans l'en-tête.

## 6. Assistant de clôture mensuelle

Menu **Clôture → Nouvelle clôture**. Six étapes guidées :

1. **Vérifier les soldes** bancaires et de caisse.
2. **Rapprocher les transactions** non rapprochées.
3. **Balance de vérification** : détecter les anomalies.
4. **Régularisations** : salaires, amortissement, charges payées
   d'avance. Vous pouvez éditer chaque ligne avant de publier.
5. **États financiers** : BS, P&L, flux de trésorerie, SOCE.
6. **Publier** : idempotent (un double-clic ne double pas les
   écritures).

## 7. Connecter QuickBooks (par client)

Menu **Clients → [Client] → Intégrations → QuickBooks** :

1. **Autoriser OAuth** QBO (un clic, redirection Intuit).
2. **Sens du flux** : bidirectionnel par défaut. Vous pouvez
   choisir *pull only* ou *push only* selon le client.
3. **Résolution de conflits** : automatique; les conflits non
   résolus apparaissent dans la file de révision.

## 8. Routage bancaire intelligent

Pour chaque client :

- Si la banque est **déjà connectée à QuickBooks**, OtoCPA tire
  automatiquement les transactions de QBO. **Aucune connexion Plaid
  nécessaire.**
- Sinon, OtoCPA invite le client à se connecter via **Plaid** (portail
  sécurisé, aucun identifiant bancaire stocké chez vous ni chez nous).

## 9. Rapports et états financiers

Menu **Rapports** :

- **Balance de vérification** (TB)
- **État des résultats** (P&L)
- **Bilan** (BS)
- **Flux de trésorerie**
- **SOCE** (état des capitaux propres)
- **Rapport par téléverseur** (qui a soumis combien de reçus ce mois)
- **Vieillissement** (A/R, A/P)

Tous exportables en PDF ou CSV (CSV avec BOM UTF-8 pour Excel).

## 10. Engagements d'audit

Menu **Audit → Nouvel engagement** :

- **Importance relative** (CAS 320)
- **Évaluation des risques** (CAS 315)
- **Échantillonnage statistique**
- **Lettres d'affirmation** (génération PDF FR ou EN)
- **Notes de revue** par compte
- **Papiers de travail** structurés

## 11. Déclarations fiscales

- **T2** (sociétés fédéral) — pré-remplissage automatique des annexes
  1, 8, 50, 100, 125.
- **CO-17** (Québec) — mappage automatique du T2.
- **TPS/TVQ** — calcul et pré-remplissage.
- **T5013** (sociétés de personnes) — feuillets par associé.
- **T661** (RS&DE) — narratifs structurés.

## 12. Messagerie client

Chaque client a un fil de messages bidirectionnel :

- Envoyer un message depuis **Clients → [Client] → Messages**.
- Choisir le destinataire (tous ou un utilisateur spécifique en
  mode multi).
- Le client reçoit une notification (courriel et / ou WhatsApp selon
  ses préférences).

## Questions fréquentes

**Q : Comment révoquer l'accès d'un employé client immédiatement?**
R : Ouvrir la fiche client → Utilisateurs du portail → bouton
*Retirer*. Les jetons sont invalidés sur-le-champ.

**Q : Un client peut-il avoir son propre administrateur?**
R : Oui, en mode multi. Le premier administrateur peut en promouvoir
d'autres.

**Q : Les anciens liens `/c/{token}` fonctionnent-ils encore?**
R : Oui en mode *single*; en mode *multi*, ils mènent à une page
« Utilisez votre lien personnel ».

## Besoin d'aide

- **Documentation** : `docs/` dans le dépôt.
- **Courriel** : support@otocpa.com
- **Dans l'application** : icône d'aide en haut à droite (raccourcis
  + visite guidée).
